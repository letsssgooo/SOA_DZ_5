//go:build integration && e2e

package integration

import (
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	eventsv1 "github.com/letsssgooo/SOA_DZ_5/gen/go/cinema/events/v1"
	"google.golang.org/protobuf/proto"
)

const aggregationRequestTimeout = 30 * time.Second

type metricRow struct {
	Name       string   `json:"metric_name"`
	MovieID    string   `json:"movie_id"`
	Value      *float64 `json:"value"`
	ComputedMS string   `json:"computed_ms"`
}

type metricKey struct{ name, movieID string }

func checkManualAggregation(t *testing.T) {
	day := freeHistoryDay(t)
	t.Logf("Контрольные события за %s, %s и %s UTC", day.Format(time.DateOnly),
		day.AddDate(0, 0, 1).Format(time.DateOnly), day.AddDate(0, 0, 7).Format(time.DateOnly))

	endpoint := postEndpoint(t, "movie-service", movieServiceContainerPort, "/events")
	userPrefix := "aggregation-e2e-" + uuid.NewString()
	a := userPrefix + "-a"
	b := userPrefix + "-b"
	start := eventsv1.MovieEvent_VIEW_STARTED
	finish := eventsv1.MovieEvent_VIEW_FINISHED
	search := eventsv1.MovieEvent_SEARCHED
	fixtures := []struct {
		dayOffset int
		user      string
		movie     string
		kind      eventsv1.MovieEvent_EventType
		seconds   int32
	}{
		{0, a, "movie-a", start, 0},
		{0, a, "movie-a", finish, 120},
		{0, b, "movie-b", start, 0},
		{0, b, "movie-b", finish, 240},
		{0, userPrefix + "-c", "movie-a", start, 0},
		{1, a, "", search, 0},
		{1, b, "", search, 0},
		{7, b, "", search, 0},
	}
	events := make([]*eventsv1.MovieEvent, 0, len(fixtures))
	for index, fixture := range fixtures {
		at := day.AddDate(0, 0, fixture.dayOffset).Add(time.Duration(index+1) * time.Minute * 5)
		event := &eventsv1.MovieEvent{
			EventId:         proto.String(uuid.NewString()),
			UserId:          proto.String(fixture.user),
			MovieId:         proto.String(fixture.movie),
			EventType:       fixture.kind.Enum(),
			Timestamp:       proto.Int64(at.UnixMilli()),
			DeviceType:      eventsv1.MovieEvent_MOBILE.Enum(),
			SessionId:       proto.String(fmt.Sprintf("%s-%d", fixture.user, fixture.dayOffset)),
			ProgressSeconds: proto.Int32(fixture.seconds),
		}
		publishEvent(t, endpoint, event)
		events = append(events, event)
	}
	for _, event := range events {
		if err := verifyStoredEvent(t.Context(), event); err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("POST /events → Kafka → ClickHouse: сверены все поля %d контрольных событий", len(events))

	want := map[metricKey]float64{
		{"dau", ""}: 3, {"avg_watch_seconds", ""}: 180, {"conversion", ""}: 2.0 / 3,
		{"movie_views", "movie-a"}: 2, {"movie_views", "movie-b"}: 1,
		{"retention_d1", ""}: 2.0 / 3, {"retention_d7", ""}: 1.0 / 3,
	}
	date := day.Format(time.DateOnly)
	aggregationURL := postEndpoint(t, "aggregation-service", "8082", "/aggregations") + "?date=" + date
	client := &http.Client{Timeout: aggregationRequestTimeout}
	for attempt := 1; attempt <= 2; attempt++ {
		request, err := http.NewRequestWithContext(t.Context(), http.MethodPost, aggregationURL, nil)
		if err != nil {
			t.Fatal(err)
		}
		response, err := client.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		response.Body.Close()
		if response.StatusCode != http.StatusNoContent {
			t.Fatalf("POST /aggregations?date=%s: HTTP %d, ожидался 204", date, response.StatusCode)
		}
		ch, pg := storedMetrics(t, day)
		if len(ch) != len(want) || !reflect.DeepEqual(ch, pg) {
			t.Fatalf("ожидалось %d одинаковых строк: ClickHouse=%d, PostgreSQL=%d, совпадение=%t",
				len(want), len(ch), len(pg), reflect.DeepEqual(ch, pg))
		}
		seen := make(map[metricKey]bool, len(ch))
		for _, metric := range ch {
			key := metricKey{metric.Name, metric.MovieID}
			expected, ok := want[key]
			if seen[key] || !ok || metric.Value == nil || *metric.Value != expected {
				t.Fatalf("неожиданная метрика %s / %s: %+v, ожидаемое значение %v", metric.Name, metric.MovieID, metric, expected)
			}
			seen[key] = true
			if attempt == 1 {
				t.Logf("Метрика %s %s: ожидалось %g, ClickHouse и PostgreSQL = %g", metric.Name, metric.MovieID, expected, *metric.Value)
			}
		}
		t.Logf("POST /aggregations?date=%s → 204, запуск %d: %d строк в каждой БД, дублей нет", date, attempt, len(ch))
	}
}

func checkScheduledAggregation(t *testing.T) {
	yesterday := time.Now().UTC().Truncate(24*time.Hour).AddDate(0, 0, -1)
	id := uuid.NewString()
	event := &eventsv1.MovieEvent{
		EventId:         proto.String(id),
		UserId:          proto.String("scheduler-e2e-" + id),
		MovieId:         proto.String(""),
		EventType:       eventsv1.MovieEvent_SEARCHED.Enum(),
		Timestamp:       proto.Int64(yesterday.UnixMilli()),
		DeviceType:      eventsv1.MovieEvent_MOBILE.Enum(),
		SessionId:       proto.String(id),
		ProgressSeconds: proto.Int32(0),
	}
	publishEvent(t, postEndpoint(t, "movie-service", movieServiceContainerPort, "/events"), event)
	if err := verifyStoredEvent(t.Context(), event); err != nil {
		t.Fatal(err)
	}
	t.Logf("Событие для планировщика %s сохранено в ClickHouse", id)
	since := time.Now().UTC()
	days := []time.Time{yesterday, yesterday.AddDate(0, 0, -1), yesterday.AddDate(0, 0, -7)}
	type cycle struct {
		Message  string  `json:"msg"`
		Date     string  `json:"date"`
		Status   string  `json:"status"`
		Events   *uint64 `json:"processed_events"`
		Duration *int64  `json:"duration_ms"`
	}
	finished := make(map[string]cycle)
	waitE2E(t, "следующий цикл планировщика и его результаты", func() bool {
		started := make(map[string]bool)
		log := runCompose(t, "logs", "--since", since.Format(time.RFC3339Nano), "--no-log-prefix", "aggregation-service")
		for line := range strings.SplitSeq(log, "\n") {
			if !strings.HasPrefix(line, "{") {
				continue
			}
			var entry cycle
			if err := json.Unmarshal([]byte(line), &entry); err != nil {
				t.Fatalf("чтение лога планировщика: %v", err)
			}
			switch entry.Message {
			case "daily aggregation started":
				started[entry.Date] = true
			case "daily aggregation finished":
				if started[entry.Date] && entry.Status == "success" && entry.Events != nil && entry.Duration != nil {
					finished[entry.Date] = entry
				}
			}
		}
		for index, day := range days {
			entry, ok := finished[day.Format(time.DateOnly)]
			if !ok || (index == 0 && *entry.Events == 0) {
				return false
			}
			ch, pg := storedMetrics(t, day)
			if !reflect.DeepEqual(ch, pg) {
				return false
			}
			var hasDAU, hasD1, hasD7 bool
			for _, metric := range ch {
				switch metric.Name {
				case "dau":
					hasDAU = metric.Value != nil && (index != 0 || *metric.Value > 0)
				case "retention_d1":
					hasD1 = true
				case "retention_d7":
					hasD7 = true
				}
			}
			if !hasDAU || hasD1 != (index >= 1) || hasD7 != (index == 2) {
				return false
			}
		}
		return true
	})
	for _, day := range days {
		entry := finished[day.Format(time.DateOnly)]
		t.Logf("Планировщик: %s, событий %d, %d мс. Срезы обеих БД совпали", entry.Date, *entry.Events, *entry.Duration)
	}
}

func freeHistoryDay(t *testing.T) time.Time {
	t.Helper()
	const query = `
SELECT DISTINCT formatDateTime(timestamp, '%F', 'UTC')
FROM cinema.movie_events
WHERE timestamp < toDate(now('UTC')) - 19`
	occupied := runCompose(t, "exec", "-T", "clickhouse", "clickhouse-client", "--query", query)
	used := make(map[string]bool)
	for day := range strings.FieldsSeq(occupied) {
		used[day] = true
	}
	today := time.Now().UTC().Truncate(24 * time.Hour)
	for day := today.AddDate(0, 0, -20); ; day = day.AddDate(0, 0, -1) {
		if !used[day.Format(time.DateOnly)] {
			return day
		}
	}
}

func storedMetrics(t *testing.T, day time.Time) (clickhouse, postgres []metricRow) {
	t.Helper()
	date := day.Format(time.DateOnly)
	ch := runCompose(t, "exec", "-T", "clickhouse", "clickhouse-client", "--param_date="+date, "--query", `
SELECT metric_name, movie_id, value, toString(toUnixTimestamp64Milli(computed_at)) AS computed_ms
FROM analytics.daily_metrics
WHERE metric_date = toDateTime64(concat({date:String}, ' 00:00:00'), 3, 'UTC')
ORDER BY metric_name, movie_id
FORMAT JSONEachRow`)
	pg := runCompose(t, "exec", "-T", "postgres", "sh", "-ec",
		`exec psql -X -U "$POSTGRES_USER" -d "$POSTGRES_DB" -At -v ON_ERROR_STOP=1 -c "$1"`, "psql",
		fmt.Sprintf(`
SELECT row_to_json(metric)
FROM (
    SELECT metric_name, movie_id, value, ((extract(epoch FROM computed_at) * 1000)::bigint)::text AS computed_ms
    FROM analytics.daily_metrics
    WHERE metric_date = DATE '%s'
    ORDER BY metric_name, movie_id
) AS metric`, date))
	return decodeMetrics(t, ch), decodeMetrics(t, pg)
}

func decodeMetrics(t *testing.T, rows string) []metricRow {
	t.Helper()
	var metrics []metricRow
	for line := range strings.SplitSeq(rows, "\n") {
		if line == "" {
			continue
		}
		var metric metricRow
		if err := json.Unmarshal([]byte(line), &metric); err != nil {
			t.Fatalf("чтение сохранённых метрик: %v", err)
		}
		metrics = append(metrics, metric)
	}
	return metrics
}
