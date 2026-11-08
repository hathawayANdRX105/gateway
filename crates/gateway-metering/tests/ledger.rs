//! Ledger tests — verify prehold, settle, and release.

use gateway_metering::ledger::{BalanceLedger, Ledger, MemoryLedger};

#[test]
fn ledger_prehold_succeeds_when_balance_sufficient() {
    let ledger = MemoryLedger::new();
    ledger.set_balance("user1", "tok1", 1000);

    let hold = ledger.prehold("user1", "tok1", 500).unwrap();
    assert_eq!(hold.amount, 500);
    assert_eq!(hold.user_key, "user1");
    assert_eq!(hold.token_key, "tok1");

    // The balance is reduced
    assert_eq!(ledger.available("user1", "tok1"), 500);
}

#[test]
fn ledger_prehold_fails_when_insufficient() {
    let ledger = MemoryLedger::new();
    ledger.set_balance("user1", "tok1", 100);

    let err = ledger.prehold("user1", "tok1", 500).unwrap_err();
    assert_eq!(err.need, 500);
    assert_eq!(err.have, 100);
}

#[test]
fn ledger_settle_refunds_difference() {
    let ledger = MemoryLedger::new();
    ledger.set_balance("user1", "tok1", 1000);

    let hold = ledger.prehold("user1", "tok1", 500).unwrap();
    // Only 300 was actually used, so 200 is refunded
    let diff = ledger.settle(&hold, 300);
    assert_eq!(diff, 200);
    assert_eq!(ledger.available("user1", "tok1"), 700);
}

#[test]
fn ledger_settle_charges_extra_when_actual_exceeds_estimate() {
    let ledger = MemoryLedger::new();
    ledger.set_balance("user1", "tok1", 1000);

    let hold = ledger.prehold("user1", "tok1", 500).unwrap();
    // 700 was actually used, so 200 is charged on top
    let diff = ledger.settle(&hold, 700);
    assert_eq!(diff, -200);
    assert_eq!(ledger.available("user1", "tok1"), 300);
}

#[test]
fn ledger_release_restores_full_amount() {
    let ledger = MemoryLedger::new();
    ledger.set_balance("user1", "tok1", 1000);

    let hold = ledger.prehold("user1", "tok1", 500).unwrap();
    ledger.release(&hold);
    assert_eq!(ledger.available("user1", "tok1"), 1000);
}

#[test]
fn ledger_available_default_zero() {
    let ledger = MemoryLedger::new();
    assert_eq!(ledger.available("user1", "tok1"), 0);
}

#[test]
fn ledger_multiple_users_isolated() {
    let ledger = MemoryLedger::new();
    ledger.set_balance("user1", "tok1", 1000);
    ledger.set_balance("user2", "tok2", 500);

    let hold = ledger.prehold("user1", "tok1", 300).unwrap();
    assert_eq!(ledger.available("user1", "tok1"), 700);
    assert_eq!(ledger.available("user2", "tok2"), 500);

    ledger.settle(&hold, 200);
    assert_eq!(ledger.available("user1", "tok1"), 800);
    assert_eq!(ledger.available("user2", "tok2"), 500);
}
#[test]
fn ledger_release_is_idempotent() {
    // Release refunds the full preheld amount. Releasing an already-released hold must not
    // double-refund: apply_hold removes the prehold record, so a second release finds no
    // record in flight and leaves the balance at the single-release result.
    let ledger = MemoryLedger::new();
    ledger.set_balance("user1", "tok1", 1000);

    let hold = ledger.prehold("user1", "tok1", 500).unwrap();
    assert_eq!(
        ledger.available("user1", "tok1"),
        500,
        "prehold reserves 500 of 1000"
    );

    ledger.release(&hold);
    assert_eq!(
        ledger.available("user1", "tok1"),
        1000,
        "first release restores the full amount"
    );

    ledger.release(&hold);
    assert_eq!(
        ledger.available("user1", "tok1"),
        1000,
        "second release is a no-op: equals single-release balance"
    );
}

#[test]
fn ledger_reseed_preserves_in_flight_holds() {
    // reseed overwrites balances from a fresh quota snapshot, but the snapshot does not
    // carry memory-side in-flight holds: a prehold taken before the reseed was deducted
    // from the old balance and the DB row knows nothing about it. reseed must subtract
    // each token's total in-flight hold amount, otherwise `available` would show money
    // that is still reserved and a concurrent request could prehold the same money twice.
    let ledger = MemoryLedger::new();
    ledger.set_balance("user1", "tok1", 1000);

    let hold = ledger.prehold("user1", "tok1", 500).unwrap();
    assert_eq!(
        ledger.available("user1", "tok1"),
        500,
        "prehold reserves 500 of 1000"
    );

    let quotas = gateway_contract::gate::QuotaSnapshot::default();
    quotas.upsert("tok1".into(), 3000);
    ledger.reseed(&quotas);
    assert_eq!(
        ledger.available("user1", "tok1"),
        2500,
        "reseed overwrote to 3000 then subtracted the 500 in-flight hold: 2500"
    );

    // 300 was actually used of the 500 prehold: 200 refunded on top of the reseeded
    // balance (the diff is computed against the prehold taken before the reseed).
    let diff = ledger.settle(&hold, 300);
    assert_eq!(diff, 200, "settle refunds the prehold/actual difference");
    assert_eq!(
        ledger.available("user1", "tok1"),
        2700,
        "hold still honored after reseed: 2500 + (500 - 300) = 2700"
    );
}
