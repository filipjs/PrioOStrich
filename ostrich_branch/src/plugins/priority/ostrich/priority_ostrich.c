#include <pthread.h>
#include <stdio.h>

#include "slurm/slurm_errno.h"

#include "src/common/slurm_priority.h"
#include "src/common/assoc_mgr.h"


const char plugin_name[]       	= "Priority OSTRICH plugin";
const char plugin_type[]       	= "priority/ostrich";
const uint32_t plugin_version	= 100;


/*********************** local structures *********************/

/*********************** local variables **********************/
static bool stop_thread = false;
static bool config_flag = false;
static bool priority_debug = false;

static pthread_t ostrich_thread = 0;
static pthread_mutex_t term_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t  term_cond = PTHREAD_COND_INITIALIZER;

static uint32_t schedule_interval;
static uint32_t threshold;

static time_t last_sched_time;		/* time of last scheduling pass */

/*********************** local functions **********************/
static void _load_config(void);
//static void _update_struct(void);
static void _my_sleep(int secs);

static void _stop_ostrich_agent(void);
static void *_ostrich_agent(void *no_data);

/*********************** operations on lists ******************/

/*********************** implementation ***********************/

// TODO


static void _load_config(void)
{
	char *select_type, *preempt_type;
	uint32_t req_job_age;

	// TODO temporary fix, since we cannot add custom parameters like in plugins/scheduler
	// FIXME interval is in minutes, we need seconds
	schedule_interval = slurm_get_priority_calc_period() / 60;
	threshold = slurm_get_priority_decay_hl();

	if (slurm_get_debug_flags() & DEBUG_FLAG_PRIO)
		priority_debug = 1;
	else
		priority_debug = 0;

	// TODO
// 	if (slurm_get_priority_favor_small())
// 		sort = sjb;
// 	else
// 		sort = fifo / longest job first??;

	if (priority_debug) {
		info("priority: Interval is %u", schedule_interval);
		info("priority: Threshold is %u", threshold);
	}

// TODO _job_resources based on "select type"
//	select_type = slurm_get_select_type();
//	if (strcmp(select_type, "select/serial"))
//		fatal("OStrich: scheduler supports only select/serial (%s given)",
//			select_type);
//	xfree(select_type);

	preempt_type = slurm_get_preempt_type();
	if (strcmp(preempt_type, "preempt/none"))
		fatal("OStrich: scheduler supports only preempt/none (%s given)", preempt_type);
	xfree(preempt_type);

	if (slurm_get_preempt_mode() != PREEMPT_MODE_OFF)
		fatal("OStrich: scheduler supports only PreemptMode=OFF");

	req_job_age = 4 * schedule_interval;
	if (slurmctld_conf.min_job_age > 0 && slurmctld_conf.min_job_age < req_job_age)
		fatal("OStrich: MinJobAge must be greater or equal to %d", req_job_age);
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

static void _stop_ostrich_agent(void)
{
	pthread_mutex_lock(&term_lock);
	stop_thread = true;
	pthread_cond_signal(&term_cond);
	pthread_mutex_unlock(&term_lock);

}

static void *_ostrich_agent(void *no_data)
{
	_load_config();

	while (!stop_thread) {
		debug("SLEEPING");
		_my_sleep(schedule_interval);
	}
	debug("OSTRICH THREAD ENDED");
}


/*
 * init() is called when the plugin is loaded, before any other functions
 * are called.  Put global initialization here.
 */
int init ( void )
{
	pthread_attr_t attr;

	debug("%s loaded", plugin_name);

	if (ostrich_thread) {
		debug2("OStrich: priority thread already running, "
			"not starting another");
		return SLURM_ERROR;
	}

	slurm_attr_init(&attr);
	if (pthread_create(&ostrich_thread, &attr, _ostrich_agent, NULL))
		fatal("OStrich: unable to start priority thread");
	slurm_attr_destroy(&attr);

	return SLURM_SUCCESS;
}

int fini ( void )
{
	debug("%s ended", plugin_name);

	if (ostrich_thread) {
		_stop_ostrich_agent();
		pthread_join(ostrich_thread, NULL);
		ostrich_thread = 0;
	}

	return SLURM_SUCCESS;
}

/*
 * The remainder of this file implements the standard SLURM priority API.
 */

extern uint32_t priority_p_set(uint32_t last_prio, struct job_record *job_ptr)
{
	return 1;

	uint32_t new_prio = 1;

	if (job_ptr->direct_set_prio && (job_ptr->priority > 1))
		return job_ptr->priority;

	if (last_prio >= 2)
		new_prio = (last_prio - 1);

	if (job_ptr->details)
		new_prio -= (job_ptr->details->nice - NICE_OFFSET);

	/* System hold is priority 0 */
	if (new_prio < 1)
		new_prio = 1;

	return new_prio;
}

extern void priority_p_reconfig(bool assoc_clear)
{
	return;
}

extern void priority_p_set_assoc_usage(slurmdb_association_rec_t *assoc)
{
	return;
}

extern double priority_p_calc_fs_factor(long double usage_efctv,
					long double shares_norm)
{
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
	return(list_create(NULL));
}

extern void priority_p_job_end(struct job_record *job_ptr)
{
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
