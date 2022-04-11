package checkscheduler

import (
	"context"
	"encoding/json"
	"hideki/que/types"

	"github.com/vgarvardt/gue/v3"
)

type checkFireJobArgs struct {
	CheckID int64
}

var (
	fireJob = CheckSchedulerJob{
		JobType: "checkFireJob",
		JobFunc: checkFireJob,
	}
)

// checkFireJob fires scheduled check
func checkFireJob(ctx context.Context, job *gue.Job) error {
	var (
		args  checkFireJobArgs
		check types.Check
	)
	if err := json.Unmarshal(job.Args, &args); err != nil {
		return err
	}
	logger.Debug("checkFireJob", "checkID", args.CheckID)

	// TODO get check
	check = types.Check{} // dummy check
	if check.Info.ScheduleStatus == nil || *check.Info.ScheduleStatus != types.CheckScheduleStatusScheduled {
		logger.Debug("checkFireJob: Invalid status", "checkID", args.CheckID)
		check.Info.ScheduleStatus = &types.CheckScheduleStatusCanceled
		// this will delete from queue without performing job
		return nil
	}

	// TODO if return error, it will be reworked. If error is not recoverable then cancel schedule.
	return nil
}

// EnqueueCheckFireJob schedules check fire job
func EnqueueCheckFireJob(check *types.Check) error {
	logger.Debug("EnqueueFireJob", "checkID", check.ID)
	var (
		args []byte
		job  *gue.Job
		err  error
	)
	if args, err = json.Marshal(checkFireJobArgs{CheckID: check.ID}); err != nil {
		return err
	}

	job = &gue.Job{
		Type:     fireJob.JobType, // registered to gue with function
		Queue:    fireJob.JobType, // queue name in database. just use job type string
		Priority: 0,               // The highest priority is -32768, the lowest one is +32767
		RunAt:    check.BeginPrepAt,
		Args:     args,
	}
	if err = client.enqueueJob(job); err != nil {
		return err
	}
	check.Info.ScheduleJobID = &job.ID
	logger.Debug("EnqueueFireJob", "checkID", check.ID, "jobID", job.ID)
	// TODO save to database
	return nil
}

// DequeueCheckFireJob cancels queued check job
func DequeueCheckFireJob(check *types.Check) error {
	logger.Debug("DequeueCheckFireJob", "checkID", check.ID)

	if check.Info.ScheduleJobID != nil {
		if err := client.dequeueJob(*check.Info.ScheduleJobID); err != nil {
			return err
		}
	}
	return nil
}
