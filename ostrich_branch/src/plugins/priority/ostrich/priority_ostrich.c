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

/* User type flags */
#define TYPE_FLAG_NORMAL 0x00000001 /* normal user */
#define TYPE_FLAG_ASSOC  0x00000002 /* account association */


const char plugin_name[]       	= "Priority OSTRICH plugin";
const char plugin_type[]       	= "priority/ostrich";
const uint32_t plugin_version	= 100;


/*********************** local structures *********************/
struct ostrich_campaign {
	uint32_t id;
	uint32_t priotity;

	uint32_t time_offset;		/* time needed to complete previous campaigns */
	uint32_t completed_time;	/* total time of completed jobs in the campaign */
	uint32_t remaining_time;	/* total time of still active jobs in the campaign */
	double virtual_time;		/* time assigned from the user's virtual time pool */

	time_t creation_time;
	List jobs;
};

struct ostrich_user {
	uint32_t id;
	uint32_t type_flag;		/* see TYPE_FLAG_* above */

	uint32_t active_campaigns;	/* number of active campaigns */
	uint32_t last_camp_id;		/* ID of the most recent campaign */

	double virtual_pool;		/* accumulated time from the virtual schedule */
	double time_share;		/* amount of time to assign to the user */

	List campaigns;
	List waiting_jobs;
};

struct ostrich_schedule {		/* one virtual schedule per partition */
	char *part_name;		/* name of the partition */
	uint16_t priority;		/* scheduling priority of the partition */
	uint32_t max_time;		/* maximum time for jobs in the partition */
	uint32_t cpus_pn;		/* average number of cpus per node in the partition */

	double total_shares;		/* total number of time shares from active users */

	List users;
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

static List ostrich_sched_list;	/* list of ostrich_schedule entries */
static List incoming_jobs;		/* list of jobs entering the system */

static time_t last_sched_time;		/* time of last scheduling pass */

/*********************** local functions **********************/
static uint32_t (*_job_resources)(struct job_record *job_ptr);
static uint32_t _serial_resources(struct job_record *job_ptr);
static uint32_t _linear_resources(struct job_record *job_ptr);
static uint32_t _cons_resources(struct job_record *job_ptr);

static void _load_config(void);
static void _update_struct(void);
static void _my_sleep(int secs);

static void *_ostrich_agent(void *no_data);
static void _stop_ostrich_agent(void);

/*********************** operations on lists ******************/
static int _list_find_schedule(struct ostrich_schedule *sched, char *name)
{
	return (strcmp(sched->part_name, name) == 0);
}

static struct ostrich_schedule *_find_schedule(char *name)
{
	return list_find_first(ostrich_sched_list,
			       (ListFindF) _list_find_schedule,
			       name);
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

/*********************** implementation ***********************/


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

		xfree(prio_params);
	} else {
		// 'fix' in versions without 'PriorityParameters'
		// calc_period is in minutes, we need seconds for interval
		schedule_interval = slurm_get_priority_calc_period() / 60;
		threshold = slurm_get_priority_decay_hl();
	}

	if (schedule_interval < 1)
		fatal("OStrich: invalid interval: %d", schedule_interval);
	if (threshold < 60)
		fatal("OStrich: invalid threshold: %d", threshold);

	info("OStrich: Interval is %u", schedule_interval);
	info("OStrich: Threshold is %u", threshold);

// TODO
// 	if (slurm_get_priority_favor_small())
// 		sort = sjb;
// 	else
// 		sort = fifo / longest job first??;

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
			sched->cpus_per_node = 1;
		else
			sched->cpus_per_node = part_ptr->total_cpus / part_ptr->total_nodes;
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

static void *_ostrich_agent(void *no_data)
{
	// TODO LOCKS

	/* Create empty lists. */
	ostrich_sched_list = list_create( (ListDelF) _list_delete_schedule );
	incoming_jobs = list_create(NULL); /* job pointers, no delete function */

	_load_config();
	_update_struct();

	while (!stop_thread) {
		debug("SLEEPING");
		_my_sleep(schedule_interval);

		struct ostrich_schedule *sched;
		ListIterator iter;

		iter = list_iterator_create(ostrich_sched_list);
		while ((sched = (struct ostrich_schedule *) list_next(iter))) {
			debug("SCHED %s %d %d", sched->part_name, sched->priority, sched->max_time);
		}
	}
	debug("OSTRICH THREAD ENDED");
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
	//TODO DODAC TUTAJ TEZ OD RAZU REZERWACJE??
	//TODO JAK SIE PRACA ZMIENI TO PO PORSTU WYWOLANE BEDZIE JESZCZE RAZ
	list_enqueue(incoming_jobs, job_ptr);
	return 1;
}

extern void priority_p_reconfig(bool assoc_clear)
{
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
