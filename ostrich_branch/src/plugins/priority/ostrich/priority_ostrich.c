#include <pthread.h>
#include <stdio.h>

#include "slurm/slurm_errno.h"

#include "src/common/assoc_mgr.h"
#include "src/common/slurm_priority.h"
#include "src/common/xmalloc.h"
#include "src/common/xstring.h"

#define INTERVAL 10
#define THRESHOLD 60 * 20

#define DEFAULT_PART_LIMIT 60 * 24 * 7
#define DEFAULT_JOB_LIMIT  60 * 24 * 7 * 365

#define DEFAULT_NORM_SHARE 0.01

/* Mode flags */
#define MODE_FLAG_ONLY_ASSOC 0x00000000 /* use only associations */
#define MODE_FLAG_NO_ASSOC   0x00000001 /* do not use associations */
#define MODE_FLAG_MIXED      0x00000002 /* use associations if present */

/* User type flags */
#define TYPE_FLAG_NORMAL 0x00000001 /* normal user */
#define TYPE_FLAG_ASSOC  0x00000002 /* account association */


const char plugin_name[]       	= "Priority OSTRICH plugin";
const char plugin_type[]       	= "priority/ostrich";
const uint32_t plugin_version	= 100;


/*********************** local structures *********************/
struct ostrich_campaign {
	uint32_t id;
	uint32_t priority;

	uint32_t time_offset;		/* time needed to complete previous campaigns */
	uint32_t completed_time;	/* total time of completed jobs in the campaign */
	uint32_t remaining_time;	/* total time of still active jobs in the campaign */
	double virtual_time;		/* time assigned from the user's virtual time pool */

	time_t accept_point;		/* campaign threshold for accepting new jobs */
	List jobs;
};

struct ostrich_user {
	uint32_t id;
	uint32_t type_flag;		/* see TYPE_FLAG_* above */

	uint32_t active_campaigns;	/* number of active campaigns */
	uint32_t last_camp_id;		/* ID of the most recent campaign */

	double virtual_pool;		/* accumulated time from the virtual schedule */
	double norm_share;		/* normalized share */

	List campaigns;
	List waiting_jobs;
};

struct ostrich_schedule {		/* one virtual schedule per partition */
	char *part_name;		/* name of the partition */
	uint16_t priority;		/* scheduling priority of the partition */
	uint32_t max_time;		/* maximum time for jobs in the partition */
	uint32_t cpus_pn;		/* average number of cpus per node in the partition */

	uint32_t working_cpus;		/* number of cpus that are currently allocated to jobs */
	double total_shares;		/* sum of shares from active users */

	List users;
};

struct user_key {
	uint32_t user_id;
	uint32_t user_type;
};

/*********************** local variables **********************/
static bool stop_thread = false;
static bool config_flag = false;
static bool priority_debug = false;

static pthread_t ostrich_thread = 0;
static pthread_mutex_t thread_flag_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t term_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t  term_cond = PTHREAD_COND_INITIALIZER;

static uint32_t schedule_interval = INTERVAL;
static uint32_t threshold = THRESHOLD;
static uint32_t mode = MODE_FLAG_ONLY_ASSOC;
static bool favor_small;

static List ostrich_sched_list;	/* list of ostrich_schedule entries */
static List incoming_jobs;		/* list of jobs entering the system */

static time_t last_sched_time;		/* time of last scheduling pass */

/*********************** local functions **********************/
static struct ostrich_schedule *_find_schedule(char *name);
static uint32_t _job_pred_runtime(struct job_record *job_ptr);
static uint32_t _job_real_runtime(struct job_record *job_ptr);
static uint32_t (*_job_resources)(struct job_record *job_ptr);
static uint32_t _serial_resources(struct job_record *job_ptr);
static uint32_t _linear_resources(struct job_record *job_ptr);
static uint32_t _cons_resources(struct job_record *job_ptr);
static int _is_job_modified(struct job_record *job_ptr,
			    struct ostrich_campaign *camp,
			    struct ostrich_user *user,
			    struct ostrich_schedule *sched);
static uint32_t _campaign_time_left(struct ostrich_campaign *camp);

static void _load_config(void);
static void _update_struct(void);
static void _my_sleep(int secs);

static void _place_waiting_job(struct job_record *job_ptr, char *part_name);
static void _manage_incoming_jobs(void);
static int _manage_waiting_jobs(struct ostrich_user *user,
				 struct ostrich_schedule *sched);
static int _update_camp_workload(struct ostrich_user *user,
				  struct ostrich_schedule *sched);
static int _distribute_time(struct ostrich_user *user, double *time_tick);
static int _update_user_activity(struct ostrich_user *user,
				  struct ostrich_schedule *sched);
static int _gather_campaigns(struct ostrich_user *user, List *l);
static void _set_multi_prio(struct job_record *job_ptr, uint32_t prio,
			    struct ostrich_schedule *sched);
static void _assign_priorities(struct ostrich_schedule *sched);

static void *_ostrich_agent(void *no_data);
static void _stop_ostrich_agent(void);

/*********************** operations on lists ******************/
static int _list_find_schedule(struct ostrich_schedule *sched, char *name)
{
	return (strcmp(sched->part_name, name) == 0);
}

static int _list_find_user(struct ostrich_user *user, struct user_key *key)
{
	return (user->id == key->user_id && user->type_flag == key->user_type);
}

static int _list_find_job(struct job_record *job_ptr, uint32_t *jid)
{
	return (job_ptr->job_id == *jid);
}

static void _list_delete_schedule(struct ostrich_schedule *sched)
{
	xassert (sched != NULL);

	xfree(sched->part_name);
	list_destroy(sched->users);
	xfree(sched);
}

static void _list_delete_user(struct ostrich_user *user)
{
	xassert (user != NULL);

	list_destroy(user->campaigns);
	list_destroy(user->waiting_jobs);
	xfree(user);
}

static void _list_delete_campaign(struct ostrich_campaign *camp)
{
	xassert (camp != NULL);

	list_destroy(camp->jobs);
	xfree(camp);
}

/* Sort in ascending order of job begin time.
 * Can be used only on lists without finished jobs. */
static int _list_sort_job_begin_time(struct job_record *x,
				      struct job_record *y)
{
	return (x->details->begin_time - y->details->begin_time);
}

/* Sort in ascending order of remaining campaign time,
 * for equal elements sort in ascending order of creation time. */
static int _list_sort_camp_remaining_time(struct ostrich_campaign *x,
					  struct ostrich_campaign *y)
{
	int diff = ( (_campaign_time_left(x) + x->time_offset) -
			(_campaign_time_left(y) + y->time_offset) );
	if (diff == 0)
		return (x->accept_point - y->accept_point);
//TODO PO WPROWADZENIU SHARES TRZEBA JESZCZE LACZNY CZAS PODZIELIC PRZEZ USER->SHARES!!!!
	return diff;
}

/* Sort in ascending order of job predicted runtime,
 * for equal elements sort in ascending order of begin time. */
static int _list_sort_job_runtime(struct job_record *x,
				  struct job_record *y)
{
	int diff = _job_pred_runtime(x) - _job_pred_runtime(y);
	if (diff == 0)
		return _list_sort_job_begin_time(x, y);
	return diff;
}

static int _list_remove_finished(struct job_record *job_ptr, void *no_data)
{
	return IS_JOB_FINISHED(job_ptr);
}

/*********************** implementation ***********************/
static struct ostrich_schedule *_find_schedule(char *name)
{
	return list_find_first(ostrich_sched_list,
			       (ListFindF) _list_find_schedule,
			       name);
}

/* _job_pred_runtime - return job time limit in seconds
 *	or DEFAULT_JOB_LIMIT if not set */
static uint32_t _job_pred_runtime(struct job_record *job_ptr)
{
	/* All values are in minutes, change to seconds. */
	if (job_ptr->time_limit == NO_VAL || job_ptr->time_limit == INFINITE)
		return DEFAULT_JOB_LIMIT * 60;
	return job_ptr->time_limit * 60;
}

/* _job_real_runtime - calculate the job run time depending on its state */
static uint32_t _job_real_runtime(struct job_record *job_ptr)
{
	time_t end;

	if (IS_JOB_FINISHED(job_ptr))
		end = job_ptr->end_time;
	else if (IS_JOB_SUSPENDED(job_ptr))
		end = job_ptr->suspend_time;
	else if (IS_JOB_RUNNING(job_ptr))
		end = time(NULL);
	else
		return 0; /* pending */

	if (end > job_ptr->start_time + job_ptr->tot_sus_time)
		return end - job_ptr->start_time - job_ptr->tot_sus_time;
	else
		return 0;
}

/* _***_resources - calculate the number of cpus used by the job */
static uint32_t _serial_resources(struct job_record *job_ptr)
{
	// TODO
	xassert (0);
	return 0;
}

static uint32_t _linear_resources(struct job_record *job_ptr)
{
	// TODO 
	xassert (0);
	return 0;
}

static uint32_t _cons_resources(struct job_record *job_ptr)
{
	if (IS_JOB_STARTED(job_ptr))
		return job_ptr->total_cpus;
	else
		/* pending, give an estimate */
		//TODO better estimate based on "select type"
		return job_ptr->details->cpus_per_task;
}

/* _is_job_modified - check if the job was modified since the last iteration */
static int _is_job_modified(struct job_record *job_ptr,
			    struct ostrich_campaign *camp,
			    struct ostrich_user *user,
			    struct ostrich_schedule *sched)
{
	struct part_record *part_ptr;
	ListIterator iter;
	int found_part = 0;

	/* Check privileges. */
	if (job_ptr->direct_set_prio || job_ptr->resv_id)
		return 1;

	/* Check association. */
	if (user->type_flag == TYPE_FLAG_ASSOC) {
		if (!job_ptr->assoc_ptr || job_ptr->assoc_id != user->id)
			return 1;
	}

	/* Check begin time. */
	if (job_ptr->details->begin_time > time(NULL) ||
		job_ptr->details->begin_time > camp->accept_point)
		return 1;

	/* Check partition. */
	if (job_ptr->part_ptr_list) {
		iter = list_iterator_create(job_ptr->part_ptr_list);
		while ((part_ptr = (struct part_record *) list_next(iter)))
			if (strcmp(part_ptr->name, sched->part_name) == 0) {
				found_part = 1;
				break;
			}
		list_iterator_destroy(iter);
	} else if (job_ptr->part_ptr) {
		found_part = (strcmp(job_ptr->part_ptr->name, sched->part_name) == 0);
	}
	if (!found_part)
		return 1;

	/* Not modified in a meaningful way. */
	return 0;
}

/* _campaign_time_left - calculate the time needed for the campaign
 *	to finish in the virtual schedule */
static uint32_t _campaign_time_left(struct ostrich_campaign *camp)
{
	uint32_t workload = camp->completed_time + camp->remaining_time;
	if (workload > (int) camp->virtual_time)
		return workload - (int) camp->virtual_time;
	return 0;
}


static void _load_config(void)
{
	char *sched_type, *select_type, *preempt_type, *tmp_ptr;
	uint32_t req_job_age;

// 	if (protocol_version >= SLURM_14_10_PROTOCOL_VERSION) // TODO FIXME
	if (1) {
		char *prio_params = slurm_get_priority_params();
	
		// from PriorityParameters: 
		// interval is in seconds, threshold in minutes
		if (prio_params && (tmp_ptr=strstr(prio_params, "interval=")))
			schedule_interval = atoi(tmp_ptr + 9);
		if (prio_params && (tmp_ptr=strstr(prio_params, "threshold=")))
			threshold = atoi(tmp_ptr + 10) * 60;
		if (prio_params && (tmp_ptr=strstr(prio_params, "mode=")))
			mode = atoi(tmp_ptr + 5);

		xfree(prio_params);
	} else {
		// 'fix' in older versions without 'PriorityParameters'
		// calc_period is in minutes, we need seconds for interval
		schedule_interval = slurm_get_priority_calc_period() / 60;
		threshold = slurm_get_priority_decay_hl();
		mode = MODE_FLAG_NO_ASSOC;
	}
	// TODO JESZCZE JEDEN IF DLA WERSJI < 2.6 I WTEDY NIE UZYWAC PRIORITY ARRAYS??

	if (schedule_interval < 1)
		fatal("OStrich: invalid interval: %d", schedule_interval);
	if (threshold < 60)
		fatal("OStrich: invalid threshold: %d", threshold);
	if (mode > 2)
		fatal("OStrich: invalid mode: %d", mode);

	if (slurm_get_priority_favor_small())
		favor_small = true;
	else
		favor_small = false;

	info("OStrich: Interval is %u", schedule_interval);
	info("OStrich: Threshold is %u", threshold);
	info("OStrich: Mode is %u", mode);
	info("OStrich: Favor small %u", favor_small);

	select_type = slurm_get_select_type();
	if (strcmp(select_type, "select/serial") == 0)
		_job_resources = _serial_resources;
	else if (strcmp(select_type, "select/linear") == 0)
		_job_resources = _linear_resources;
	else if (strcmp(select_type, "select/cons_res") == 0)
		_job_resources = _cons_resources;
	else
		xassert (0); // TODO FIXME A CO DLA SELECT/CRAY SELECT/BLUEGENE??
	xfree(select_type);

	if (slurm_get_debug_flags() & DEBUG_FLAG_PRIO)
		priority_debug = 1;
	else
		priority_debug = 0;
	
	sched_type = slurm_get_sched_type();
	if (strcmp(sched_type, "sched/builtin") &&
	    strcmp(sched_type, "sched/backfill"))
		fatal("OStrich: supports only sched/builtin or sched/backfill");
	xfree(sched_type);

	preempt_type = slurm_get_preempt_type();
	if (strcmp(preempt_type, "preempt/none"))
		fatal("OStrich: supports only preempt/none");
	xfree(preempt_type);

	if (slurm_get_preempt_mode() != PREEMPT_MODE_OFF)
		fatal("OStrich: supports only PreemptMode=OFF");

	req_job_age = 4 * schedule_interval;
	if (slurmctld_conf.min_job_age > 0 && slurmctld_conf.min_job_age < req_job_age)
		fatal("OStrich: MinJobAge must be greater or equal to %d", req_job_age);
}

/* _update_struct - update the internal list of virtual schedules,
 *	keep one schedule per existing partition
 * global: part_list - pointer to global partition list
 */
static void _update_struct(void)
{
	struct ostrich_schedule *sched;
	struct part_record *part_ptr;
	ListIterator iter;

	/* Remove unnecessary ostrich_schedule entries. */
	iter = list_iterator_create(ostrich_sched_list);

	while ((sched = (struct ostrich_schedule *) list_next(iter))) {
		part_ptr = find_part_record(sched->part_name);
		if (!part_ptr)
			list_delete_item(iter);
	}

	list_iterator_destroy(iter);

	/* Create missing ostrich_schedule entries. */
	iter = list_iterator_create(part_list);

	while ((part_ptr = (struct part_record *) list_next(iter))) {
		sched = _find_schedule(part_ptr->name);
		if (!sched) {
			sched = xmalloc(sizeof(struct ostrich_schedule));
			sched->part_name = xstrdup(part_ptr->name);
			sched->working_cpus = 0;
			sched->total_shares = 0;

			sched->users = list_create( (ListDelF) _list_delete_user );

			list_append(ostrich_sched_list, sched);
		}
		/* Set/update priority. */
		sched->priority = part_ptr->priority;
		/* Set/update max_time. */
		if (part_ptr->max_time == NO_VAL || part_ptr->max_time == INFINITE)
			sched->max_time = DEFAULT_PART_LIMIT;
		else
			sched->max_time = part_ptr->max_time;
		/* Set/update average cpu count. */
		if (part_ptr->total_nodes >= part_ptr->total_cpus)
			sched->cpus_pn = 1;
		else
			sched->cpus_pn = part_ptr->total_cpus / part_ptr->total_nodes;
	}

	list_iterator_destroy(iter);
}

static void _my_sleep(int secs)
{
	struct timespec ts = {0, 0};

	ts.tv_sec = time(NULL) + secs;
	pthread_mutex_lock(&term_lock);
	if (!stop_thread)
		pthread_cond_timedwait(&term_cond, &term_lock, &ts);
	pthread_mutex_unlock(&term_lock);
}


/* _place_waiting_job - put the job to the owners waiting list in the specified partition */
static void _place_waiting_job(struct job_record *job_ptr, char *part_name)
{
	slurmdb_association_rec_t *assoc;
	struct ostrich_schedule *sched;
	struct ostrich_user *user;
	struct user_key key;

	sched = _find_schedule(part_name);

	xassert(sched != NULL);

	if (mode == MODE_FLAG_ONLY_ASSOC) {
		if (job_ptr->assoc_ptr) {
			key.user_id = job_ptr->assoc_id;
			key.user_type = TYPE_FLAG_ASSOC;
		} else {
			error("OStrich: skipping job %d, no account association",
			      job_ptr->job_id);
			// TODO JOB_PRIO = 0 I JAKIS REASON??
			return;
		}
	} else if (mode == MODE_FLAG_NO_ASSOC) {
		key.user_id = job_ptr->user_id;
		key.user_type = TYPE_FLAG_NORMAL;
	} else {  // mixed mode
		if (job_ptr->assoc_ptr) {
			key.user_id = job_ptr->assoc_id;
			key.user_type = TYPE_FLAG_ASSOC;
		} else {
			key.user_id = job_ptr->user_id;
			key.user_type = TYPE_FLAG_NORMAL;
			debug("OStrich: mixed mode, job %d without association",
			      job_ptr->job_id);
		}
	}

	user = list_find_first(sched->users,
			       (ListFindF) _list_find_user,
			       &key);

	if (!user) {
		user = xmalloc(sizeof(struct ostrich_user));

		user->id = key.user_id;
		user->type_flag = key.user_type;

		user->active_campaigns = 0;
		user->last_camp_id = 0;

		user->virtual_pool = 0;
		user->norm_share = 0;

		user->campaigns = list_create( (ListDelF) _list_delete_campaign );
		user->waiting_jobs = list_create(NULL); /* job pointers, no delete function */

		list_append(sched->users, user);
	}

	/* Set/update normalized share. */
	if (user->type_flag == TYPE_FLAG_ASSOC) {
		assoc = (slurmdb_association_rec_t *) job_ptr->assoc_ptr;
		user->norm_share = assoc->usage->shares_norm;
		//TODO CZY TO MOZE BYC ZERO?? MOZE JEDNAK USTAWIC JAKIES DEFAULTOWE MINIMUM??
	} else
		user->norm_share = DEFAULT_NORM_SHARE;

	list_append(user->waiting_jobs, job_ptr);
}

/* _manage_incoming_jobs - move newly submitted jobs to the appropriate schedules */
static void _manage_incoming_jobs(void)
{
	struct part_record *part_ptr;
	struct job_record *job_ptr;
	ListIterator iter;

	while ((job_ptr = (struct job_record *) list_dequeue(incoming_jobs))) {
		if (job_ptr->part_ptr_list) {
			iter = list_iterator_create(job_ptr->part_ptr_list);
			while ((part_ptr = (struct part_record *) list_next(iter)))
				_place_waiting_job(job_ptr, part_ptr->name);
			list_iterator_destroy(iter);
		} else if (job_ptr->part_ptr) {
			_place_waiting_job(job_ptr, job_ptr->part_ptr->name);
		} else {
			error("OStrich: skipping job %d, no partition specified",
			      job_ptr->job_id);
			// TODO JOB_PRIO = 0 I JAKIS REASON??
		}
	}
}

/* _manage_waiting_jobs - move jobs from waiting list to the matching campaigns */
static int _manage_waiting_jobs(struct ostrich_user *user,
				 struct ostrich_schedule *sched)
{
	struct ostrich_campaign *camp;
	struct job_record *job_ptr, *dup;
	ListIterator camp_iter, job_iter;
	time_t now = time(NULL);
	bool orig_list_ended = false;

	/* Remove finished jobs from the waiting list. */
	list_delete_all(user->waiting_jobs,
			(ListFindF) _list_remove_finished,
			NULL);
	/* Now we can sort by begin time. */
	list_sort(user->waiting_jobs,
		  (ListCmpF) _list_sort_job_begin_time);

	camp_iter = list_iterator_create(user->campaigns);
	job_iter = list_iterator_create(user->waiting_jobs);

	camp = (struct ostrich_campaign *) list_next(camp_iter);
	job_ptr = (struct job_record *) list_next(job_iter);

	/* Assign waiting jobs to existing campaigns or create new
	 * campaigns. Don't bother with jobs that cannot run now.
	 * Note: campaigns are already sorted by creation time.
	 */
	while(job_ptr && job_ptr->details->begin_time <= now) {
		if (!camp) { /* create a new campaign */
			camp = xmalloc(sizeof(struct ostrich_campaign));

			camp->id = ++user->last_camp_id;
			camp->priority = 0;

			camp->time_offset = 0;
			camp->completed_time = 0;
			camp->remaining_time = 0;
			camp->virtual_time = 0;

			camp->accept_point = job_ptr->details->begin_time + threshold;
			camp->jobs = list_create(NULL); /* job pointers, no delete function */

			list_append(user->campaigns, camp);
			/* We iterated through the whole original list of campaigns
			 * and now we cannot continue to use 'camp_iter'. */
			orig_list_ended = true;
		}
		if (job_ptr->details->begin_time <= camp->accept_point) {
			/* Look for duplicates in the campaign. */
			dup = list_find_first(camp->jobs,
					      (ListFindF) _list_find_job,
					      &(job_ptr->job_id));
			if (!dup)
				list_append(camp->jobs, job_ptr);
			/* Advance to the next job. */
			list_remove(job_iter);
			job_ptr = (struct job_record *) list_next(job_iter);
		} else if (!orig_list_ended) {
			/* Advance to the next campaign. */
			camp = (struct ostrich_campaign *) list_next(camp_iter);
		} else {
			/* This will force new campaigns if there are any jobs left. */
			camp = NULL;
		}
	}
	/* Note: if a remaining job from the waiting list is updated,
	 * it will be re-introduced to the system. 
	 * There is no need to validate it.
	 */
	list_iterator_destroy(camp_iter);
	list_iterator_destroy(job_iter);
	return 0;
}

//TODO OPIS
//* Check the boundaries of all the jobs in the campaigns.
// 	 * Calculate the number of allocated cpus in this partition. */
static int _update_camp_workload(struct ostrich_user *user,
				 struct ostrich_schedule *sched)
{
	struct ostrich_campaign *camp;
	struct job_record *job_ptr;
	ListIterator camp_iter, job_iter;
	uint32_t job_cpus, job_runtime, job_time_limit;

	camp_iter = list_iterator_create(user->campaigns);

	while ((camp = (struct ostrich_campaign *) list_next(camp_iter))) {

		camp->remaining_time = 0;
		job_iter = list_iterator_create(camp->jobs);

		while ((job_ptr = (struct job_record *) list_next(job_iter))) {

			job_cpus = _job_resources(job_ptr);
			job_runtime = _job_real_runtime(job_ptr);
			job_time_limit = MIN(_job_pred_runtime(job_ptr),
					     sched->max_time);
			/* Remove finished jobs. */
			if (IS_JOB_FINISHED(job_ptr)) {
				/* Use real time. */
				camp->completed_time += job_runtime * job_cpus;

// TODO
// 				if (DEBUG_FLAG)
// 					_print_finished_job(job_ptr, camp->camp_id);
 
				list_remove(job_iter);
				continue;
			}
			/* Validate the job */
			if (_is_job_modified(job_ptr, camp, user, sched)) {
				list_remove(job_iter);
				continue;
			}
			/* Note: if a job is updated, it will be re-introduced to the system. 
			* There is no need to manually put it back to 'incoming_jobs'.
			*/

			/* Job belongs to the campaign, process it further. */
			if (IS_JOB_SUSPENDED(job_ptr)) {
				/* Use real time. */
				camp->remaining_time += job_runtime * job_cpus;
			} else if (IS_JOB_RUNNING(job_ptr)) {
				/* Use predicted time. */
				camp->remaining_time += job_time_limit * job_cpus;
				/* Add the resources only if the job is running. */
				sched->working_cpus += job_cpus;
			} else  {
				/* Job is pending, use predicted time. */
				camp->remaining_time += job_time_limit * job_cpus;
			}
		}

		list_iterator_destroy(job_iter);
	}

	list_iterator_destroy(camp_iter);
	return 0;
}

static int _distribute_time(struct ostrich_user *user, double *time_tick)
{
	if (user->active_campaigns)
		user->virtual_pool += (*time_tick * user->norm_share);
	return 0;
}

//TODO OPIS FIXME
/* _check_user_activity - further processing of the user on the given partition.
 *	4) Redistribute virtual time between campaigns so that earlier created
 *	campaigns finish first in the virtual schedule.
 *	5) Count shares of active users.
 */
static int _update_user_activity(struct ostrich_user *user,
				  struct ostrich_schedule *sched)
{
	struct ostrich_campaign *camp;
	ListIterator iter;
	double virt_time;
	uint32_t workload, offset = 0;

	user->active_campaigns = 0;
	iter = list_iterator_create(user->campaigns);

	/* Combine the virtual time from all campaigns. */
	while ((camp = (struct ostrich_campaign *) list_next(iter))) {
		user->virtual_pool  += camp->virtual_time;
		camp->virtual_time = 0;
	}

	list_iterator_reset(iter);

	/* Redistribute the virtual time by following the (default)
	 * campaign ordering by creation time. */
	while ((camp = (struct ostrich_campaign *) list_next(iter))) {
		workload = camp->completed_time + camp->remaining_time;

		virt_time = MIN((double) workload, user->virtual_pool);

		camp->virtual_time += virt_time;
		user->virtual_pool -= virt_time;

		/* Offset is the total virtual time still
		 * needed by the previous campaigns. */
		camp->time_offset = offset;
		offset += _campaign_time_left(camp);

		if (_campaign_time_left(camp) == 0) {
			/* Campaign ended in the virtual schedule. */
			if (list_is_empty(camp->jobs) &&
				time(NULL) > camp->accept_point)
				/* It also ended in the real schedule. */
				list_delete_item(iter);
		} else {
			/* Campaign is active. */
			user->active_campaigns++;
		}
	}

	list_iterator_destroy(iter);

	if (user->active_campaigns)
		sched->total_shares += user->norm_share;
	/* The virtual time overflow is lost if not fully redistributed.
	 * This is done to prevent potential abuses of the system. */
	user->virtual_pool = 0;
	return 0;
}

/* _gather_campaigns - merge campaigns into one common list */
static int _gather_campaigns(struct ostrich_user *user, List *l)
{
	list_append_list(*l, user->campaigns);
	return 0;
}

/* _set_multi_prio - set the priority of the job to 'prio'.
 * Note: priority is set only for the given partition by using priority_array.
 */
static void _set_multi_prio(struct job_record *job_ptr, uint32_t prio,
			    struct ostrich_schedule *sched)
{
	struct part_record *part_ptr;
	ListIterator iter;
	int inx = 0, size;

	if (job_ptr->part_ptr_list) {
		if (!job_ptr->priority_array) {
			size = list_count(job_ptr->part_ptr_list) * sizeof(uint32_t);
			job_ptr->priority_array = xmalloc(size);
			/* If the partition count changes in the future,
			 * the priority_array will be deallocated automatically
			 * by SLURM in job_mgr.c 'update_job'.
			 */
		}
		iter = list_iterator_create(job_ptr->part_ptr_list);
		while ((part_ptr = (struct part_record *) list_next(iter))) {
			if (strcmp(part_ptr->name, sched->part_name) == 0)
				job_ptr->priority_array[inx] = prio;
				/* Continue on with the loop, there might be
				 *  multiple entries of the same partition. */
			inx++;
		}
		list_iterator_destroy(iter);
	}
	if (job_ptr->part_ptr) {
		if (strcmp(job_ptr->part_ptr->name, sched->part_name) == 0)
			job_ptr->priority = prio;
	}
}

/* _assign_priorities - set the priority for all the jobs in the partition. */
static void _assign_priorities(struct ostrich_schedule *sched)
{
	struct ostrich_campaign *camp;
	struct job_record *job_ptr;
	List all_campaigns;
	ListIterator camp_iter, job_iter;
	int prio, normal_queue =  500000; /* Starting priority. */

	all_campaigns = list_create(NULL); /* Tmp list, no delete function. */

	list_for_each(sched->users,
		      (ListForF) _gather_campaigns,
		      &all_campaigns);

	/* Sort the combined list of campaigns. */
	list_sort(all_campaigns,
		  (ListCmpF) _list_sort_camp_remaining_time);

	camp_iter = list_iterator_create(all_campaigns);

	while ((camp = (struct ostrich_campaign *) list_next(camp_iter))) {
		
		if (favor_small)
			/* Sort the jobs inside each campaign. */
			list_sort(camp->jobs,
				  (ListCmpF) _list_sort_job_runtime);

		job_iter = list_iterator_create(camp->jobs);
		while ((job_ptr = (struct job_record *) list_next(job_iter))) {
			/* Take into account partition priority. */
			prio = normal_queue-- + sched->priority;
			if (prio < 1)
				prio = 1;
			_set_multi_prio(job_ptr, prio, sched);
		}
		list_iterator_destroy(job_iter);
	}

	list_iterator_destroy(camp_iter);
	list_destroy(all_campaigns);
}


static void *_ostrich_agent(void *no_data)
{
	struct ostrich_schedule *sched;
	ListIterator iter;
	uint32_t prev_allocated;
	double time_skipped;
	DEF_TIMERS;

	/* Read config, and partitions; Write jobs. */
	slurmctld_lock_t all_locks = {
		READ_LOCK, WRITE_LOCK, NO_LOCK, READ_LOCK };

	/* Create empty lists. */
	ostrich_sched_list = list_create( (ListDelF) _list_delete_schedule );
	incoming_jobs = list_create(NULL); /* job pointers, no delete function */

	/* Read settings. */
	_load_config();

	last_sched_time = time(NULL);

	while (!stop_thread) {
		_my_sleep(schedule_interval);

		if (stop_thread)
			break;
		
		lock_slurmctld(all_locks);

		START_TIMER;

		if (config_flag) {
			config_flag = false;
			_load_config();
		}

		_update_struct();
		_manage_incoming_jobs();

		time_skipped = difftime(time(NULL), last_sched_time);
		last_sched_time = time(NULL);
		
		iter = list_iterator_create(ostrich_sched_list);
		
		while ((sched = (struct ostrich_schedule *) list_next(iter))) {

			list_for_each(sched->users,
				      (ListForF) _manage_waiting_jobs,
				      sched);
			
			prev_allocated = sched->working_cpus;
			sched->working_cpus = 0;
			
			list_for_each(sched->users,
				      (ListForF) _update_camp_workload,
				      sched);
			
			sched->working_cpus = MAX(sched->working_cpus, 1);

			if (sched->total_shares > 0) {
				time_skipped *= MAX(prev_allocated, sched->working_cpus);
				time_skipped /= sched->total_shares;

				list_for_each(sched->users,
					      (ListForF) _distribute_time,
					      &time_skipped);
			}

			sched->total_shares = 0;
			
			list_for_each(sched->users,
				      (ListForF) _update_user_activity,
				      sched);
			
			_assign_priorities(sched);
		}

		list_iterator_destroy(iter);
		//TODO USUWANIE OSTRICH_USER JESLI INACTIVE PRZEZ DLUGI CZAS???
		unlock_slurmctld(all_locks);

		END_TIMER2("OStrich: ostrich_agent");
		debug2("OStrich: schedule iteration %s", TIME_STR);
	}

	debug("OSTRICH THREAD ENDED"); // TODO DELETE
	/* Cleanup. */
	list_destroy(ostrich_sched_list);
	list_destroy(incoming_jobs);
	return NULL;
}

static void _stop_ostrich_agent(void)
{
	pthread_mutex_lock(&term_lock);
	stop_thread = true;
	pthread_cond_signal(&term_cond);
	pthread_mutex_unlock(&term_lock);
}


/*
 * init() is called when the plugin is loaded, before any other functions
 * are called.  Put global initialization here.
 */
int init ( void )
{
	pthread_attr_t attr;

	verbose("OStrich: plugin loaded");

	pthread_mutex_lock(&thread_flag_lock);
	if (ostrich_thread) {
		debug2("OStrich: priority thread already running, "
			"not starting another");
		pthread_mutex_unlock(&thread_flag_lock);
		return SLURM_ERROR;
	}

	slurm_attr_init(&attr);
	if (pthread_create(&ostrich_thread, &attr, _ostrich_agent, NULL))
		fatal("OStrich: unable to start priority thread");
	slurm_attr_destroy(&attr);

	pthread_mutex_unlock(&thread_flag_lock);
	return SLURM_SUCCESS;
}

int fini ( void )
{
	verbose("OStrich: plugin shutting down");

	pthread_mutex_lock(&thread_flag_lock);
	if (ostrich_thread) {
		_stop_ostrich_agent();
		pthread_join(ostrich_thread, NULL);
		ostrich_thread = 0;
	}

	pthread_mutex_unlock(&thread_flag_lock);
	return SLURM_SUCCESS;
}

/*
 * The remainder of this file implements the standard SLURM priority API.
 */

extern uint32_t priority_p_set(uint32_t last_prio, struct job_record *job_ptr)
{
	// NOTE: must be called while holding slurmctld_lock_t
	if (job_ptr->direct_set_prio)
		return job_ptr->priority;
	//TODO DODAC TUTAJ TEZ OD RAZU REZERWACJE
	//TODO JAK SIE PRACA ZMIENI TO PO PORSTU WYWOLANE BEDZIE JESZCZE RAZ
// 			has_resv1 = (job_rec1->job_ptr->resv_id != 0); TODO
	list_enqueue(incoming_jobs, job_ptr);
	return 1;
}

extern void priority_p_reconfig(bool assoc_clear)
{
	// TODO FIXME TO NIE JEST POD SLURM LOCKIEM, ALBO NAPRAWIC ALBO DODAC WLASNY LOCK!!
	config_flag = true;
	return;
}

extern void priority_p_set_assoc_usage(slurmdb_association_rec_t *assoc)
{
	// TODO DOUBLE CHECK JAKIE SA LOCKI, CZY POTRZEBA JEDNAK MUTEXA??
	return;
}

extern double priority_p_calc_fs_factor(long double usage_efctv,
					long double shares_norm)
{
	// TODO DOUBLE CHECK JAKIE SA LOCKI, CZY POTRZEBA JEDNAK MUTEXA??
	return 1;

	/* This calculation is needed for sshare when ran from a
	 * non-multifactor machine to a multifactor machine.  It
	 * doesn't do anything on regular systems, it should always
	 * return 0 since shares_norm will always be NO_VAL. */
// 	double priority_fs;
//
// 	xassert(!fuzzy_equal(usage_efctv, NO_VAL));
//
// 	if ((shares_norm <= 0.0) || fuzzy_equal(shares_norm, NO_VAL))
// 		priority_fs = 0.0;
// 	else
// 		priority_fs = pow(2.0, -(usage_efctv / shares_norm));
//
// 	return priority_fs;
}

extern List priority_p_get_priority_factors_list(
	priority_factors_request_msg_t *req_msg, uid_t uid)
{
	// TODO DOUBLE CHECK JAKIE SA LOCKI, CZY POTRZEBA JEDNAK MUTEXA??
	return(list_create(NULL));
}

extern void priority_p_job_end(struct job_record *job_ptr)
{
	// TODO DOUBLE CHECK JAKIE SA LOCKI, CZY POTRZEBA JEDNAK MUTEXA??
	return;

	//TODO
	uint64_t unused_cpu_run_secs = 0;
	uint64_t time_limit_secs = (uint64_t)job_ptr->time_limit * 60;
	slurmdb_association_rec_t *assoc_ptr;
	assoc_mgr_lock_t locks = { WRITE_LOCK, NO_LOCK,
				   WRITE_LOCK, NO_LOCK, NO_LOCK };

	/* No unused cpu_run_secs if job ran past its time limit */
	if (job_ptr->end_time >= job_ptr->start_time + time_limit_secs)
		return;

	unused_cpu_run_secs = job_ptr->total_cpus *
		(job_ptr->start_time + time_limit_secs - job_ptr->end_time);

	assoc_mgr_lock(&locks);
	if (job_ptr->qos_ptr) {
		slurmdb_qos_rec_t *qos_ptr =
			(slurmdb_qos_rec_t *)job_ptr->qos_ptr;
		if (unused_cpu_run_secs >
		    qos_ptr->usage->grp_used_cpu_run_secs) {
			qos_ptr->usage->grp_used_cpu_run_secs = 0;
			debug2("acct_policy_job_fini: "
			       "grp_used_cpu_run_secs "
			       "underflow for qos %s", qos_ptr->name);
		} else
			qos_ptr->usage->grp_used_cpu_run_secs -=
				unused_cpu_run_secs;
	}
	assoc_ptr = (slurmdb_association_rec_t *)job_ptr->assoc_ptr;
	while (assoc_ptr) {
		/* If the job finished early remove the extra time now. */
		if (unused_cpu_run_secs >
		    assoc_ptr->usage->grp_used_cpu_run_secs) {
			assoc_ptr->usage->grp_used_cpu_run_secs = 0;
			debug2("acct_policy_job_fini: "
			       "grp_used_cpu_run_secs "
			       "underflow for account %s",
			       assoc_ptr->acct);
		} else {
			assoc_ptr->usage->grp_used_cpu_run_secs -=
				unused_cpu_run_secs;
			debug4("acct_policy_job_fini: job %u. "
			       "Removed %"PRIu64" unused seconds "
			       "from assoc %s "
			       "grp_used_cpu_run_secs = %"PRIu64"",
			       job_ptr->job_id, unused_cpu_run_secs,
			       assoc_ptr->acct,
			       assoc_ptr->usage->grp_used_cpu_run_secs);
		}
		/* now handle all the group limits of the parents */
		assoc_ptr = assoc_ptr->usage->parent_assoc_ptr;
	}
	assoc_mgr_unlock(&locks);

	return;
}
