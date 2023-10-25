package bloomgateway

import (
	"time"

	"github.com/grafana/loki/pkg/logproto"
	"github.com/prometheus/common/model"
)

func getDay(ts model.Time) int64 {
	return ts.Unix() / int64(24*time.Hour/time.Second)
}

func getDayTime(ts model.Time) time.Time {
	return time.Date(ts.Time().Year(), ts.Time().Month(), ts.Time().Day(), 0, 0, 0, 0, time.UTC)
}

func filterRequestForDay(r *logproto.FilterChunkRefRequest, day int64) *logproto.FilterChunkRefRequest {
	var from, through time.Time
	refs := make([]*logproto.GroupedChunkRefs, 0, len(r.Refs))
	for i := range r.Refs {
		groupedChunkRefs := &logproto.GroupedChunkRefs{
			Fingerprint: r.Refs[i].Fingerprint,
			Tenant:      r.Refs[i].Tenant,
			Refs:        make([]*logproto.ShortRef, 0, len(r.Refs[i].Refs)),
		}
		for j := range r.Refs[i].Refs {
			shortRef := r.Refs[i].Refs[j]
			fromDay := getDay(shortRef.From)
			if fromDay > day {
				break
			}
			throughDay := getDay(shortRef.Through)
			if fromDay == day || throughDay == day {
				groupedChunkRefs.Refs = append(groupedChunkRefs.Refs, shortRef)
			}
		}
		groupFrom, groupThrough := getFromThrough(groupedChunkRefs.Refs)
		if from.Unix() > 0 && groupFrom.Before(from) {
			from = groupFrom
		}
		if groupThrough.After(through) {
			through = groupThrough
		}
		refs = append(refs, groupedChunkRefs)
	}
	return &logproto.FilterChunkRefRequest{
		From:    model.TimeFromUnix(from.Unix()),
		Through: model.TimeFromUnix(through.Unix()),
		Refs:    refs,
		Filters: r.Filters,
	}
}
