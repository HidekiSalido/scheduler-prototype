package types

import "time"

type CheckScheduleStatus string

// TODO more status may be needed for schedule management
var (
	CheckScheduleStatusPending   CheckScheduleStatus = "Pending"
	CheckScheduleStatusScheduled CheckScheduleStatus = "Scheduled"
	CheckScheduleStatusCanceled  CheckScheduleStatus = "Canceled"
)

type CheckInfo struct {
	ScheduleStatus *CheckScheduleStatus
	ScheduleJobID  *int64
}

type Check struct {
	ID          int64
	BeginPrepAt time.Time
	ScheduledAt time.Time
	Info        CheckInfo
}
