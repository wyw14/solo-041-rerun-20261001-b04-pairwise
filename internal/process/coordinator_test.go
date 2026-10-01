package process

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/wyw14/cry-97/internal/model"
)

func newTestPlant(t *testing.T, lines ...model.LineID) *Plant {
	t.Helper()
	plant, err := NewPlant(t.TempDir(), lines, func() time.Time { return time.Now() })
	if err != nil {
		t.Fatalf("new plant: %v", err)
	}
	return plant
}

func batchCreatedEvents(t *testing.T, plant *Plant, lineID model.LineID) int {
	t.Helper()
	events, err := plant.store.Events(context.Background(), lineID)
	if err != nil {
		t.Fatalf("read journal: %v", err)
	}
	count := 0
	for _, event := range events {
		if event.Kind == model.EventBatchCreated {
			count++
		}
	}
	return count
}

// A conflicting second request must not change the line's intake admission,
// the equalizer assignment, or the journal.
func TestStartBatchConflictLeavesIntakePoolAndJournalUntouched(t *testing.T) {
	plant := newTestPlant(t, "L1")
	ctx := context.Background()

	if _, err := plant.StartBatch(ctx, "L1", 400, "river"); err != nil {
		t.Fatalf("first start: %v", err)
	}
	before, err := plant.Status("L1")
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if before.Intake == nil || before.Intake.FlowM3Hour != 400 || before.Intake.Source != "river" {
		t.Fatalf("unexpected admission after first start: %+v", before.Intake)
	}

	if _, err := plant.StartBatch(ctx, "L1", 900, "industrial"); err == nil {
		t.Fatal("expected conflict for second start on a line with an active batch")
	}

	after, err := plant.Status("L1")
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if after.Intake == nil || after.Intake.FlowM3Hour != 400 || after.Intake.Source != "river" {
		t.Fatalf("rejected request changed intake admission: %+v", after.Intake)
	}
	if len(after.Equalizers) != len(before.Equalizers) {
		t.Fatalf("equalizer count changed: %d -> %d", len(before.Equalizers), len(after.Equalizers))
	}
	for i := range before.Equalizers {
		if before.Equalizers[i] != after.Equalizers[i] {
			t.Fatalf("rejected request changed equalizer %s: before=%+v after=%+v",
				before.Equalizers[i].ID, before.Equalizers[i], after.Equalizers[i])
		}
	}
	if count := batchCreatedEvents(t, plant, "L1"); count != 1 {
		t.Fatalf("expected exactly 1 batch.created event, got %d", count)
	}
	if _, ok := plant.ActiveBatch("L1"); !ok {
		t.Fatal("original batch is no longer active after rejected request")
	}
}

// Concurrent creations on one line: exactly one succeeds, and the recorded
// admission and equalizer assignment belong to the winning request.
func TestStartBatchConcurrentOnlyOneSucceeds(t *testing.T) {
	plant := newTestPlant(t, "L1")
	ctx := context.Background()

	const attempts = 16
	type outcome struct {
		batch model.Batch
		flow  float64
	}
	results := make(chan outcome, attempts)
	var wg sync.WaitGroup
	for i := 0; i < attempts; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			flow := 100 + float64(i)
			batch, err := plant.StartBatch(ctx, "L1", flow, "source")
			if err == nil {
				results <- outcome{batch: batch, flow: flow}
			}
		}(i)
	}
	wg.Wait()
	close(results)

	var winners []outcome
	for result := range results {
		winners = append(winners, result)
	}
	if len(winners) != 1 {
		t.Fatalf("expected exactly one successful creation, got %d", len(winners))
	}
	winner := winners[0]

	active, ok := plant.ActiveBatch("L1")
	if !ok || active.ID != winner.batch.ID {
		t.Fatalf("active batch does not match the winning request: %+v vs %+v", active, winner.batch)
	}
	status, err := plant.Status("L1")
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if status.Intake == nil || status.Intake.FlowM3Hour != winner.flow {
		t.Fatalf("admission does not match the winning request: %+v (want flow %v)", status.Intake, winner.flow)
	}
	assigned := 0
	for _, basin := range status.Equalizers {
		if basin.LineID == "L1" {
			assigned++
			if basin.AssignedFlow != winner.flow {
				t.Fatalf("equalizer %s flow %v does not match winning flow %v", basin.ID, basin.AssignedFlow, winner.flow)
			}
		}
	}
	if assigned != 1 {
		t.Fatalf("expected exactly one equalizer assigned to the line, got %d", assigned)
	}
	if count := batchCreatedEvents(t, plant, "L1"); count != 1 {
		t.Fatalf("expected exactly 1 batch.created event, got %d", count)
	}
}

// A rejected request must not disturb intake validation for other lines, and
// the conflict check must not mask intake errors on a line with no batch.
func TestStartBatchIntakeValidationStillApplies(t *testing.T) {
	plant := newTestPlant(t, "L1", "L2")
	ctx := context.Background()

	if _, err := plant.StartBatch(ctx, "L1", 400, "river"); err != nil {
		t.Fatalf("start L1: %v", err)
	}
	if _, err := plant.StartBatch(ctx, "L2", 5000, "river"); err == nil {
		t.Fatal("expected flow validation error on idle line")
	}
	if _, found := plant.intake.Current("L2"); found {
		t.Fatal("failed request recorded an admission on idle line")
	}
	for _, basin := range plant.equalizers.Snapshot() {
		if basin.LineID == "L2" {
			t.Fatalf("failed request assigned equalizer %s to idle line", basin.ID)
		}
	}
	if _, err := plant.StartBatch(ctx, "L2", 500, "canal"); err != nil {
		t.Fatalf("start L2 after failed attempt: %v", err)
	}
}
