package cmd

import (
	"sync"

	"github.com/majd/ipatool/v2/pkg/appstore"
)

type downloadQueueItem struct {
	job     *DownloadJob
	req     downloadRequestPayload
	account appstore.Account
	profile *guiAccountProfile
	release func()
}

type downloadQueueManager struct {
	sync.Mutex
	running *DownloadJob
	queue   []*downloadQueueItem
}

var guiDownloadQueue = &downloadQueueManager{
	queue: make([]*downloadQueueItem, 0),
}

func (q *downloadQueueManager) enqueue(item *downloadQueueItem) (int, bool) {
	q.Lock()
	defer q.Unlock()

	if q.running == nil {
		q.running = item.job
		item.job.QueuePosition = 0
		go q.process(item)
		return 0, true
	}

	item.job.Status = "queued"
	pos := len(q.queue) + 1
	item.job.QueuePosition = pos
	q.queue = append(q.queue, item)
	return pos, false
}

func (q *downloadQueueManager) process(item *downloadQueueItem) {
	defer item.release()
	executeDownloadJob(item.job, item.req, item.account, item.profile)

	q.Lock()
	defer q.Unlock()

	q.running = nil

	for len(q.queue) > 0 {
		next := q.queue[0]
		q.queue = q.queue[1:]

		// Re-number remaining queued items
		for i, waiting := range q.queue {
			waiting.job.QueuePosition = i + 1
			activeJobs.update(waiting.job.ID, func(j *DownloadJob) {
				j.QueuePosition = i + 1
			})
		}

		if next.job.Status == "error" || next.job.Status == "completed" {
			next.release()
			continue
		}

		q.running = next.job
		next.job.QueuePosition = 0
		activeJobs.update(next.job.ID, func(j *DownloadJob) {
			j.QueuePosition = 0
		})
		go q.process(next)
		break
	}
}

func (q *downloadQueueManager) cancel(jobID string) bool {
	q.Lock()
	defer q.Unlock()

	for i, item := range q.queue {
		if item.job.ID == jobID {
			item.job.Status = "error"
			item.job.Error = "Đã hủy khỏi hàng chờ / Cancelled from queue"
			item.job.QueuePosition = 0
			activeJobs.update(jobID, func(j *DownloadJob) {
				j.Status = "error"
				j.Error = "Đã hủy khỏi hàng chờ / Cancelled from queue"
				j.QueuePosition = 0
			})

			item.release()
			q.queue = append(q.queue[:i], q.queue[i+1:]...)

			for j, waiting := range q.queue {
				waiting.job.QueuePosition = j + 1
				activeJobs.update(waiting.job.ID, func(job *DownloadJob) {
					job.QueuePosition = j + 1
				})
			}
			return true
		}
	}

	return false
}

func (q *downloadQueueManager) snapshot() (*DownloadJob, []*DownloadJob) {
	q.Lock()
	defer q.Unlock()

	var runningCopy *DownloadJob
	if q.running != nil {
		copyJob := *q.running
		runningCopy = &copyJob
	}

	queueCopy := make([]*DownloadJob, len(q.queue))
	for i, item := range q.queue {
		copyJob := *item.job
		queueCopy[i] = &copyJob
	}
	return runningCopy, queueCopy
}
