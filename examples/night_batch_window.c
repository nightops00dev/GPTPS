/* SPDX-License-Identifier: MIT */
/* Copyright (c) 2026 nightops00dev. See LICENSE for the full text. */
/*
 * night_batch_window.c - the nightly ERP batch window, end to end:
 * a host-owned job graph, a named-resource ramp, a per-item ledger, and a
 * bounded morning re-drive of dead letters. Modeled on the scenario in
 * https://github.com/Fikoko/GPTPS/issues/12 .
 *
 * The design rules this example follows (each was argued out in #12):
 *   - The ENGINE owns admission, execution, retries and events. The HOST owns
 *     the clock, the job graph and the morning report. GPTPS has no clock and
 *     no DAG: the host gates on SUCCESS (inventory -> mrp -> rollup), not on
 *     "ended somehow" (that is what gptps_orch means, and it is the wrong
 *     gate here).
 *   - Dead letters are re-driven at most once (bounded). The re-drive happens
 *     in the gptps_dead_letter_drain callback, where re-submitting is safe.
 *   - The ramp is a NAMED RESOURCE re-budget, not limits.max_concurrent_tasks
 *     (that one is the worker pool: fixed at open, and a live write changes
 *     nothing since e19a058). A raise admits waiting work at once; a cut
 *     never cancels running work.
 *   - Task bodies are PURE functions of their payload, so no body shares
 *     mutable state with the host thread. The ledger lives entirely on the
 *     host thread: outcomes come from gptps_await_wait, re-drives from the
 *     drain callback (also the host thread), the report from the ledger.
 *     The event callback only prints, like demo.c, and never touches state.
 *
 * Failure shape: INV-3 is submitted as "FAIL:INV-3" and fails every attempt;
 * after its retries exhaust it lands in the dead-letter list, and the morning
 * drain re-submits it once under its real id. Re-drives are at-least-once, so
 * a real invoice task must be idempotent - the payload marker stands in for
 * "the first attempt already issued it".
 *
 *   cmake --build build --target example_night_batch   (in this repo)
 *   or: sh tools/amalgamate.sh out --addons await && \
 *       cc -std=c99 night_batch_window.c out/gptps.c out/gptps_await.c \
 *          -Iout -Iinclude -lpthread -ldl
 */
#include "gptps.h"
#include "gptps_await.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define JOB_N 9 /* inventory, mrp, 4 invoices, 2 reports, rollup */
#define WAIT_MS 10000

typedef struct {
    char name[16];
    gptps_handle h;
    gptps_status outcome; /* written only by the host thread */
    int submitted;
} ledger_row;

typedef struct {
    ledger_row row[JOB_N];
    size_t n;
    int redriven; /* one bounded morning re-drive, host thread only */
} host;

static host g; /* single host thread: no locking needed anywhere below */

/* --- task bodies: pure functions of the payload, no shared state -------- */

static gptps_status inventory(gptps_ctx *ctx, void *ud)
{
    unsigned long snap = 42;
    (void)ud;
    return gptps_result_set(ctx, &snap, sizeof snap);
}

static gptps_status mrp(gptps_ctx *ctx, void *ud)
{
    unsigned long plan = 7;
    (void)ud;
    return gptps_result_set(ctx, &plan, sizeof plan);
}

static gptps_status rollup(gptps_ctx *ctx, void *ud)
{
    static const char ok[] = "rollup-ran";
    (void)ud;
    return gptps_result_set(ctx, ok, sizeof ok); /* incl. NUL: proves the body ran */
}

static gptps_status invoice(gptps_ctx *ctx, void *ud)
{
    size_t n, i;
    const char *p = (const char *)gptps_payload(ctx, &n);
    unsigned long sum = 0;
    (void)ud;
    if (n >= 4 && memcmp(p, "FAIL", 4) == 0) return GPTPS_E_TASK; /* bad night */
    for (i = 0; i < n; ++i) sum += (unsigned char)p[i];
    return gptps_result_set(ctx, &sum, sizeof sum);
}

static gptps_status report(gptps_ctx *ctx, void *ud)
{
    static const char ok[] = "pdf";
    (void)ud;
    return gptps_result_set(ctx, ok, sizeof ok);
}

/* --- observer: print only. State belongs to the host thread. ------------ */

static void on_event(const gptps_event *ev, void *ud)
{
    const char *k;
    (void)ud;
    switch (ev->kind) {
        case GPTPS_EV_QUEUED:        k = "queued";        break;
        case GPTPS_EV_STARTED:       k = "started";       break;
        case GPTPS_EV_FINISHED:      k = "finished";      break;
        case GPTPS_EV_FAILED:        k = "failed";        break;
        case GPTPS_EV_RETRIED:       k = "retried";       break;
        case GPTPS_EV_DEAD_LETTERED: k = "dead-lettered"; break;
        default:                     k = "dropped";       break;
    }
    printf("  [event] %-13s task=%-9s attempt=%u status=%s\n",
           k, ev->task_name, ev->attempt, gptps_strerror(ev->status));
}

/* --- ledger helpers (host thread only) ----------------------------------- */

static ledger_row *ledger_add(const char *name, gptps_handle h)
{
    ledger_row *r = &g.row[g.n++];
    snprintf(r->name, sizeof r->name, "%s", name);
    r->h = h;
    r->outcome = GPTPS_OK;
    r->submitted = 1;
    return r;
}

static ledger_row *ledger_by_handle(gptps_handle h)
{
    size_t i;
    for (i = 0; i < g.n; ++i) if (g.row[i].h == h) return &g.row[i];
    return NULL;
}

/* The morning re-drive: one attempt, then give up. The payload is only valid
 * for this call, so copy what you need before returning. Re-submitting here
 * is safe: the drain callback runs with the engine lock released. */
static void redrive(const gptps_dead_letter *dl, void *ud)
{
    gptps *e = (gptps *)ud;
    char id[32];
    size_t n = dl->payload_len < sizeof id - 1 ? dl->payload_len : sizeof id - 1;
    gptps_handle h;
    if (g.redriven) return; /* bounded: exactly one re-drive */
    memcpy(id, dl->payload, n);
    id[n] = '\0';
    if (n >= 5 && memcmp(id, "FAIL:", 5) == 0) {
        memmove(id, id + 5, n - 5); /* strip the marker: second run, clean payload */
        id[n - 5] = '\0';
    }
    if (gptps_submit(e, "invoice", id, strlen(id), &h) == GPTPS_OK) {
        ledger_row *r = ledger_by_handle(dl->handle);
        g.redriven = 1;
        if (r) r->h = h; /* the row now belongs to the new handle */
        printf("  [drain ] re-submitted '%s' after dead letter (status=%s)\n",
               id, gptps_strerror(dl->status));
    }
}

/* --- the window ----------------------------------------------------------- */

int main(void)
{
    gptps *e = NULL;
    gptps_await *aw = NULL;
    gptps_config cfg;
    gptps_task_def d;
    gptps_status st = GPTPS_OK;
    void *res = NULL;
    size_t res_len = 0;
    ledger_row *r_mrp, *r_roll = NULL, *r3;
    int ok = 0, i;

#define REQUIRE(expr) do { if (!(expr)) { \
    fprintf(stderr, "line %d: %s\n", __LINE__, #expr); goto cleanup; \
} } while (0)

    memset(&g, 0, sizeof g);
    memset(&cfg, 0, sizeof cfg);
    cfg.struct_size = sizeof cfg;
    cfg.limits.struct_size = sizeof cfg.limits;
    cfg.limits.max_concurrent_tasks = 2;      /* the 16-core box, scaled down */
    cfg.limits.max_memory_bytes = 64u << 20;
    REQUIRE(gptps_open_ex(&cfg, &e) == GPTPS_OK);
    gptps_set_event_cb(e, on_event, NULL);

    /* The budget starts tight (one "core"); the ramp at 01:00 widens it.
     * MRP costs 2, so before the ramp only single-slot jobs can be admitted. */
    REQUIRE(gptps_define_resource(e, "cpu", 1) == GPTPS_OK);
    REQUIRE(gptps_set_task_resource_cost(e, "mrp", "cpu", 2) == GPTPS_OK);

    /* The wait must be installed BEFORE the first submit: completions can
     * race the submit itself, and only an already-registered observer can
     * close that race. */
    aw = gptps_await_install(e);
    REQUIRE(aw != NULL);

    memset(&d, 0, sizeof d);
    d.struct_size = sizeof d;
    d.exec = GPTPS_EXEC_INPROC;
    d.default_cost.struct_size = sizeof d.default_cost;
    d.default_cost.mem_bytes = 1u << 20;
    d.default_policy.struct_size = sizeof d.default_policy;
    d.default_policy.max_retries = 1;
    d.default_policy.on_failure = GPTPS_ON_FAILURE_DEAD_LETTER;

    d.name = "inventory"; d.run = inventory;
    REQUIRE(gptps_register_task(e, &d) == GPTPS_OK);
    d.name = "mrp";       d.run = mrp;
    d.default_cost.mem_bytes = 4u << 20;
    REQUIRE(gptps_register_task(e, &d) == GPTPS_OK);
    d.name = "rollup";    d.run = rollup;   d.default_cost.mem_bytes = 1u << 20;
    REQUIRE(gptps_register_task(e, &d) == GPTPS_OK);
    d.name = "invoice";   d.run = invoice;
    REQUIRE(gptps_register_task(e, &d) == GPTPS_OK);
    d.name = "report";    d.run = report;
    d.default_policy.max_retries = 0;
    REQUIRE(gptps_register_task(e, &d) == GPTPS_OK);
    /* Reports yield to everything else; priorities are stamped at submit. */
    REQUIRE(gptps_set_task_priority(e, "report", -5) == GPTPS_OK);

    /* 00:00 - inventory gates the whole night: nothing else is submitted
     * until it finishes, and a failed inventory means abort, not retry. */
    {
        gptps_handle h_inv;
        REQUIRE(gptps_submit(e, "inventory", NULL, 0, &h_inv) == GPTPS_OK);
        ledger_add("inventory", h_inv);
        REQUIRE(gptps_await_wait(aw, h_inv, WAIT_MS, NULL, NULL, &st) == GPTPS_OK);
        REQUIRE(st == GPTPS_OK);
    }

    /* 01:00 - the ramp: re-budgeting an existing resource is the live
     * throttle. A raise admits waiting work at once; nothing here waits. */
    REQUIRE(gptps_define_resource(e, "cpu", 2) == GPTPS_OK);

    /* MRP next; invoices and reports backfill behind it. */
    {
        gptps_handle h_mrp;
        REQUIRE(gptps_submit(e, "mrp", NULL, 0, &h_mrp) == GPTPS_OK);
        r_mrp = ledger_add("mrp", h_mrp);
    }
    for (i = 1; i <= 4; ++i) {
        char id[16];
        gptps_handle h;
        snprintf(id, sizeof id, "INV-%d", i);
        if (i == 3) memcpy(id, "FAIL:INV-3", 11); /* tonight's casualty */
        REQUIRE(gptps_submit(e, "invoice", id, strlen(id), &h) == GPTPS_OK);
        ledger_add(id, h);
    }
    for (i = 1; i <= 2; ++i) {
        char id[16];
        gptps_handle h;
        snprintf(id, sizeof id, "RPT-%d", i);
        REQUIRE(gptps_submit(e, "report", id, strlen(id), &h) == GPTPS_OK);
        ledger_add(id, h);
    }

    /* The rollup is gated on MRP's SUCCESS, not merely its end: a failed
     * MRP means the rollup is withheld and the night is flagged. */
    REQUIRE(gptps_await_wait(aw, r_mrp->h, WAIT_MS, NULL, NULL, &st) == GPTPS_OK);
    REQUIRE(st == GPTPS_OK);
    {
        gptps_handle h_roll;
        REQUIRE(gptps_submit(e, "rollup", "after-mrp", 9, &h_roll) == GPTPS_OK);
        r_roll = ledger_add("rollup", h_roll);
    }

    /* Wait out every submitted item. INV-3 must fail into the dead letter;
     * everything else must finish. */
    for (i = 0; i < (int)g.n; ++i) {
        ledger_row *r = &g.row[i];
        if (r == r_roll) {
            REQUIRE(gptps_await_wait(aw, r->h, WAIT_MS, &res, &res_len, &st) == GPTPS_OK);
            REQUIRE(st == GPTPS_OK);
            REQUIRE(res_len == sizeof "rollup-ran" &&
                    memcmp(res, "rollup-ran", res_len) == 0); /* the body really ran */
            free(res); res = NULL;
            r->outcome = GPTPS_OK;
        } else if (strncmp(r->name, "FAIL", 4) == 0) {
            REQUIRE(gptps_await_wait(aw, r->h, WAIT_MS, NULL, NULL, &st) == GPTPS_OK);
            REQUIRE(st != GPTPS_OK); /* terminal failure: expect a dead letter */
            r->outcome = st;
        } else {
            REQUIRE(gptps_await_wait(aw, r->h, WAIT_MS, NULL, NULL, &st) == GPTPS_OK);
            REQUIRE(st == GPTPS_OK);
            r->outcome = GPTPS_OK;
        }
    }

    /* 06:30 - morning report + one bounded re-drive of the dead letters. */
    printf("\nMORNING REPORT\n");
    for (i = 0; i < (int)g.n; ++i) {
        printf("  %-12s %s\n", g.row[i].name,
               g.row[i].outcome == GPTPS_OK ? "finished"
                                            : gptps_strerror(g.row[i].outcome));
    }
    REQUIRE(gptps_dead_letter_count(e) == 1);
    REQUIRE(gptps_dead_letter_drain(e, redrive, e) == 1);

    /* The re-driven invoice is a NEW handle; its row already points at it. */
    for (i = 0; i < (int)g.n; ++i)
        if (strcmp(g.row[i].name, "FAIL:INV-3") == 0) break;
    REQUIRE(i < (int)g.n);
    r3 = &g.row[i];
    REQUIRE(gptps_await_wait(aw, r3->h, WAIT_MS, NULL, NULL, &st) == GPTPS_OK);
    REQUIRE(st == GPTPS_OK);
    r3->outcome = GPTPS_OK;
    printf("  re-drive     finished\n");

    REQUIRE(gptps_dead_letter_count(e) == 0);
    for (i = 0; i < (int)g.n; ++i)
        REQUIRE(g.row[i].outcome == GPTPS_OK);
    ok = 1;

cleanup:
    if (e) gptps_shutdown(e);       /* drains, bounded by shutdown_grace_ms */
    if (aw) gptps_await_close(aw);  /* AFTER shutdown, per the header contract */
    free(res);
    printf("night_batch_window: %s\n", ok ? "PASS" : "FAIL");
    return ok ? 0 : 1;
}
