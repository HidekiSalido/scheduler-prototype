package checkscheduler

import (
	"context"
	"fmt"

	"hideki/que/repos"
	"hideki/que/types"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/vgarvardt/gue/v3"
	"github.com/vgarvardt/gue/v3/adapter/pgxv4"
	"golang.org/x/sync/errgroup"
)

const (
	workerCount = 2
)

type CheckScheduler interface {
	EnqueueCheckScheduleJob(check *types.Check) (err error)
	DequeueCheckScheduleJob(check *types.Check) (err error)
}

type CheckSchedulerJob struct {
	JobType string
	JobFunc func(context.Context, *gue.Job) error
}

var (
	client    checkSchedulerClient
	isRunning bool
)

type checkSchedulerClient struct {
	gr        repos.GlobalRepository
	ctx       context.Context
	gueClient *gue.Client
}

func MustRun(gr repos.GlobalRepository, connection string) {
	logger.Debug("MustRun called", "isRunning", isRunning)
	// sanity check
	if isRunning {
		return
	}

	ctx := context.Background()

	// connection
	pgxCfg, err := pgxpool.ParseConfig(connection)
	if err != nil {
		panic(fmt.Errorf("unable to run checkscheduler: %w", err))
	}

	pgxPool, err := pgxpool.ConnectConfig(ctx, pgxCfg)
	if err != nil {
		panic(fmt.Errorf("unable to run checkscheduler: %w", err))
	}

	poolAdapter := pgxv4.NewConnPool(pgxPool)
	gueClient := gue.NewClient(poolAdapter)
	wm := gue.WorkMap{
		fireJob.JobType: fireJob.JobFunc, // register check fire job
	}

	// create a pool with workers
	workers := gue.NewWorkerPool(gueClient, wm, workerCount)

	// work jobs in goroutine
	group, gctx := errgroup.WithContext(ctx)
	group.Go(func() error {
		return workers.Run(gctx)
	})

	client = checkSchedulerClient{gr, ctx, gueClient}

	isRunning = true
}

func (c checkSchedulerClient) enqueueJob(job *gue.Job) error {
	if err := c.gueClient.Enqueue(c.ctx, job); err != nil {
		return err
	}
	return nil
}

func (c checkSchedulerClient) dequeueJob(jobID int64) error {
	var (
		err error
		job *gue.Job
	)
	if job, err = c.gueClient.LockJobByID(c.ctx, jobID); err != nil {
		return err
	}
	// call delete and done to be removed from database
	if err := job.Delete(c.ctx); err != nil {
		return err
	}
	if err := job.Done(c.ctx); err != nil {
		return err
	}
	return nil
}
