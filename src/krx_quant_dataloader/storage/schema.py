"""
Storage schemas

Defines minimal table schemas for relational storage. Actual DDL enforcement
is in writers.py (SQLiteSnapshotWriter._ensure_tables).

Suggested schemas:

change_rates_snapshot:
  - TRD_DD TEXT NOT NULL          (injected by preprocessing)
  - ISU_SRT_CD TEXT NOT NULL      (security ID)
  - ISU_ABBRV TEXT                (security abbreviation)
  - BAS_PRC INTEGER               (previous adjusted close, coerced)
  - TDD_CLSPRC INTEGER            (today's close, coerced)
  - CMPPREVDD_PRC TEXT            (change from previous day, passthrough)
  - FLUC_RT TEXT                  (fluctuation rate, passthrough)
  - ACC_TRDVOL INTEGER            (accumulated trade volume, coerced)
  - ACC_TRDVAL INTEGER            (accumulated trade value, coerced)
  - FLUC_TP TEXT                  (fluctuation type, passthrough)
  PRIMARY KEY (TRD_DD, ISU_SRT_CD)

change_rates_adj_factor:
  - TRD_DD TEXT NOT NULL          (trading date)
  - ISU_SRT_CD TEXT NOT NULL      (security ID)
  - ADJ_FACTOR TEXT               (adjustment factor as decimal string; empty for first observation)
  PRIMARY KEY (TRD_DD, ISU_SRT_CD)

These are suggestions; actual schemas are created and enforced by concrete writers.
"""


