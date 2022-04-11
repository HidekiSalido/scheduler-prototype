package main

import (
	"context"
	"fmt"
	"time"

	"hideki/que/checkscheduler"
	"hideki/que/repos"
	"hideki/que/types"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/vgarvardt/gue/v3/adapter/pgxv4"
)

const (
	connection = "postgres://postgres:postgres@localhost:5438/postgres?connect_timeout=180&sslmode=disable"
)

func cleanDb() error {

	var (
		err     error
		pgxCfg  *pgxpool.Config
		pgxPool *pgxpool.Pool
	)
	ctx := context.Background()

	if pgxCfg, err = pgxpool.ParseConfig(connection); err != nil {
		return err
	}
	if pgxPool, err = pgxpool.ConnectConfig(ctx, pgxCfg); err != nil {
		return err
	}

	pool := pgxv4.NewConnPool(pgxPool)

	_, er := pool.Exec(context.Background(), "TRUNCATE TABLE gue_jobs")
	if er != nil {
		return er
	}
	fmt.Println("db is cleaned up")
	return pool.Close()
}

func main() {
	var (
		err       error
		checkASAP = &types.Check{
			ID:          1,
			BeginPrepAt: time.Now().UTC(),
		}
		checkScheduled1 = &types.Check{
			ID:          2,
			BeginPrepAt: time.Now().UTC().Add(5 * time.Second),
		}
		checkScheduled2 = &types.Check{
			ID:          3,
			BeginPrepAt: time.Now().UTC().Add(5 * time.Second),
		}
	)

	if err = cleanDb(); err != nil {
		fmt.Println(err.Error())
		return
	}

	checkscheduler.MustRun(repos.GlobalRepository{}, connection)

	if err = checkscheduler.EnqueueCheckFireJob(checkASAP); err != nil {
		fmt.Println(err.Error())
		return
	}

	if err = checkscheduler.EnqueueCheckFireJob(checkScheduled1); err != nil {
		fmt.Println(err.Error())
		return
	}

	if err = checkscheduler.EnqueueCheckFireJob(checkScheduled2); err != nil {
		fmt.Println(err.Error())
		return
	}

	if err = checkscheduler.DequeueCheckFireJob(checkScheduled1); err != nil {
		fmt.Println(err.Error())
		return
	}

	time.Sleep(10 * time.Second) // wait for job done

	// try to dequeu already done job
	if err = checkscheduler.DequeueCheckFireJob(checkASAP); err != nil {
		fmt.Println(err.Error())
		return
	}

	time.Sleep(10 * time.Minute) // wait for while
}
