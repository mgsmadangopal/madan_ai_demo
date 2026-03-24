import os
import random
import uuid

import numpy as np
import pandas as pd
from faker import Faker

from utils import save_to_parquet

# Set environment variables for Databricks Volumes
import os
os.environ['CATALOG'] = 'main'
os.environ['SCHEMA'] = 'madan_gopal'
os.environ['VOLUME'] = 'raw_data'



# =====================================
# ReNew Capital Partners - Renewable Portfolio Demo
# Raw data generation (story-driven)
# =====================================

SEED = 42
np.random.seed(SEED)
random.seed(SEED)
fake = Faker()
Faker.seed(SEED)

# ---- Scale factor (MANDATORY) ----
try:
    raw_scale = os.environ.get("SCALE_FACTOR", "1.0")
    SCALE_FACTOR = float(raw_scale)
except ValueError:
    SCALE_FACTOR = 1.0
if SCALE_FACTOR <= 0 or SCALE_FACTOR > 1:
    SCALE_FACTOR = 1.0

# ---- Story time window ----
RANGE_START = pd.Timestamp("2025-10-15")
RANGE_END = pd.Timestamp("2025-12-15")
DAYS = pd.date_range(RANGE_START, RANGE_END, freq="D")

ANOMALY_START = pd.Timestamp("2025-11-21")
ROLLOUT_START = pd.Timestamp("2025-11-20")
ROLLOUT_END = pd.Timestamp("2025-11-21")
HOTFIX_DATE = pd.Timestamp("2025-12-08")
IMPACT_END = pd.Timestamp("2025-12-15")

# ---- Plant / portfolio design ----
# 8 plants: 3 affected TX solar, 2 other solar, 3 wind
PLANTS = [
    # affected solar (TX)
    {"plant_id": "PLT-TX-SOL-001", "plant_name": "Lone Star Solar A", "state": "TX", "region": "ERCOT", "asset_type": "solar", "affected": True},
    {"plant_id": "PLT-TX-SOL-002", "plant_name": "Lone Star Solar B", "state": "TX", "region": "ERCOT", "asset_type": "solar", "affected": True},
    {"plant_id": "PLT-TX-SOL-003", "plant_name": "Lone Star Solar C", "state": "TX", "region": "ERCOT", "asset_type": "solar", "affected": True},
    # other solar
    {"plant_id": "PLT-NM-SOL-004", "plant_name": "Desert Sun Solar", "state": "NM", "region": "SPP", "asset_type": "solar", "affected": False},
    {"plant_id": "PLT-OK-SOL-005", "plant_name": "Prairie Light Solar", "state": "OK", "region": "SPP", "asset_type": "solar", "affected": False},
    # wind
    {"plant_id": "PLT-TX-WND-006", "plant_name": "Panhandle Wind", "state": "TX", "region": "ERCOT", "asset_type": "wind", "affected": False},
    {"plant_id": "PLT-OK-WND-007", "plant_name": "Sooner Wind", "state": "OK", "region": "SPP", "asset_type": "wind", "affected": False},
    {"plant_id": "PLT-NM-WND-008", "plant_name": "Mesa Wind", "state": "NM", "region": "SPP", "asset_type": "wind", "affected": False},
]

AFFECTED_PLANTS = [p["plant_id"] for p in PLANTS if p["affected"]]

# Baseline net MWh/day per plant sums to ~18,500
BASELINE_MWH_BY_PLANT = {
    "PLT-TX-SOL-001": 1750.0,
    "PLT-TX-SOL-002": 1700.0,
    "PLT-TX-SOL-003": 1650.0,
    "PLT-NM-SOL-004": 1750.0,
    "PLT-OK-SOL-005": 1475.0,
    "PLT-TX-WND-006": 4200.0,
    "PLT-OK-WND-007": 3900.0,
    "PLT-NM-WND-008": 2075.0,
}

# Post-anomaly target sums to ~15,900 (primarily degrade the 3 affected TX solar; partial recovery after hotfix)
POST_EVENT_MWH_BY_PLANT = {
    "PLT-TX-SOL-001": 1300.0,
    "PLT-TX-SOL-002": 1250.0,
    "PLT-TX-SOL-003": 1200.0,
    "PLT-NM-SOL-004": 1735.0,
    "PLT-OK-SOL-005": 1465.0,
    "PLT-TX-WND-006": 4185.0,
    "PLT-OK-WND-007": 3885.0,
    "PLT-NM-WND-008": 1880.0,
}

# After hotfix (partial recovery), move affected plants halfway back to baseline
HOTFIX_MWH_BY_PLANT = {
    k: (POST_EVENT_MWH_BY_PLANT[k] + BASELINE_MWH_BY_PLANT[k]) / 2.0 if k in AFFECTED_PLANTS else POST_EVENT_MWH_BY_PLANT[k]
    for k in BASELINE_MWH_BY_PLANT
}

# Equipment counts (kept within ~900-1400 total)
# - solar: inverters, wind: turbines
EQUIPMENT_COUNTS = {
    "PLT-TX-SOL-001": 120,
    "PLT-TX-SOL-002": 110,
    "PLT-TX-SOL-003": 105,
    "PLT-NM-SOL-004": 125,
    "PLT-OK-SOL-005": 95,
    "PLT-TX-WND-006": 95,
    "PLT-OK-WND-007": 85,
    "PLT-NM-WND-008": 70,
}

STATUS_VALUES = np.array(["online", "offline", "derated", "tripped", "maintenance", "comms_lost"], dtype=object)

ALARM_CODES = np.array(
    [
        "TRIP-31",
        "TRIP-44",
        "TRIP-12",
        "DERATE-12",
        "DERATE-09",
        "DERATE-21",
        "COMMS-01",
        "COMMS-07",
        "MAINT-01",
        "GRID-02",
        "GRID-08",
        "TEMP-05",
        "VOLT-03",
        "FREQ-02",
        "OTHER-00",
    ],
    dtype=object,
)

ISSUE_CATEGORIES = np.array(
    [
        "inverter_trip",
        "comms",
        "reactive_power",
        "transformer",
        "blade_pitch",
        "gearbox",
        "inspection",
        "other",
    ],
    dtype=object,
)

VENDORS = np.array(["XG_Power", "TurbineWorks", "GridOps", "SolarFieldServices", "Other"], dtype=object)


def _normalize_probs(p: np.ndarray) -> np.ndarray:
    p = np.array(p, dtype=float)
    s = float(p.sum())
    if s <= 0:
        p = np.ones_like(p, dtype=float)
        s = float(p.sum())
    return p / s


def _choose(values, probs, size: int):
    probs = _normalize_probs(np.array(probs, dtype=float))
    return np.random.choice(values, size=int(size), p=probs)


def _day_phase(day: pd.Timestamp) -> str:
    if day < ANOMALY_START:
        return "baseline"
    if day < HOTFIX_DATE:
        return "impact"
    return "post_hotfix"


def _solar_daylight_profile(hours: np.ndarray) -> np.ndarray:
    # bell-ish curve between 6 and 18, peak near 13
    x = (hours - 13.0) / 3.2
    prof = np.exp(-0.5 * x * x)
    prof[(hours < 6) | (hours > 18)] = 0.0
    return prof


def generate_eam_asset_contract_master(row_count_target: int) -> tuple[pd.DataFrame, pd.DataFrame]:
    print(f"Generating eam_asset_contract_master (~{row_count_target:,})...")

    # Build equipment list
    equipment_rows = []
    for p in PLANTS:
        pid = p["plant_id"]
        n_eq = int(max(1, int(EQUIPMENT_COUNTS[pid] * SCALE_FACTOR)))
        if p["asset_type"] == "solar":
            eq_type = "inverter"
            # models: affected plants skew heavily to XG-440
            if p["affected"]:
                model_probs = [0.85, 0.10, 0.05]
            else:
                model_probs = [0.35, 0.45, 0.20]
            models = _choose(np.array(["XG-440", "XG-360", "Other"], dtype=object), model_probs, n_eq)
        else:
            eq_type = "turbine"
            models = np.array([None] * n_eq, dtype=object)

        # vendor: align with equipment type/model
        vendor = np.empty(n_eq, dtype=object)
        if eq_type == "inverter":
            # XG models -> XG_Power mostly, some SolarFieldServices
            v_probs = [0.82, 0.05, 0.05, 0.06, 0.02]
            vendor[:] = _choose(VENDORS, v_probs, n_eq)
            # ensure XG-440 is overwhelmingly XG_Power
            xg440 = models == "XG-440"
            if xg440.any():
                vendor[xg440] = _choose(np.array(["XG_Power", "SolarFieldServices"], dtype=object), [0.92, 0.08], int(xg440.sum()))
        else:
            vendor[:] = _choose(VENDORS, [0.03, 0.90, 0.03, 0.01, 0.03], n_eq)

        # contract type per plant
        if p["affected"]:
            contract_type = "merchant"
            merchant_exposure = np.random.uniform(0.75, 1.0)
        else:
            contract_type = _choose(np.array(["merchant", "ppa", "hedge"], dtype=object), [0.45, 0.30, 0.25], 1)[0]
            if contract_type == "merchant":
                merchant_exposure = np.random.uniform(0.60, 0.95)
            elif contract_type == "hedge":
                merchant_exposure = np.random.uniform(0.20, 0.55)
            else:
                merchant_exposure = np.random.uniform(0.05, 0.25)

        asset_id = "AST-SOLAR" if p["asset_type"] == "solar" else "AST-WIND"
        if p["affected"]:
            asset_id = "AST-SOLAR-TX"

        baseline_mwh = float(BASELINE_MWH_BY_PLANT[pid])
        dist_target = float(np.random.uniform(1.10, 1.35))
        effective_start = pd.Timestamp("2020-01-01") + pd.Timedelta(days=int(np.random.randint(0, 900)))

        for i in range(n_eq):
            equipment_id = f"EQ-{pid.split('-')[-1]}-{i + 1:04d}"
            equipment_rows.append(
                {
                    "asset_id": asset_id,
                    "plant_id": pid,
                    "plant_name": p["plant_name"],
                    "state": p["state"],
                    "region": p["region"],
                    "asset_type": p["asset_type"],
                    "equipment_id": equipment_id,
                    "equipment_type": eq_type,
                    "inverter_model": models[i],
                    "vendor": vendor[i],
                    "contract_type": contract_type,
                    "merchant_exposure_pct": float(np.round(merchant_exposure, 4)),
                    "baseline_net_mwh_per_day": float(np.round(baseline_mwh, 3)),
                    "distribution_target_coverage": float(np.round(dist_target, 3)),
                    "effective_start_date": pd.Timestamp(effective_start.date()),
                    "effective_end_date": pd.NaT,
                }
            )

    eam = pd.DataFrame(equipment_rows)

    # slight power-law skew by duplicating a small portion of equipment rows across plants (contract records)
    # to hit requested row_count while keeping keys coherent
    target = int(max(1, int(row_count_target * SCALE_FACTOR)))
    if len(eam) < target:
        extra = target - len(eam)
        dup_idx = np.random.choice(np.arange(len(eam)), size=int(extra), replace=True)
        dup = eam.iloc[dup_idx].copy()
        # make dup rows distinct by adjusting effective_start_date slightly (keys stay same as master snapshots)
        offsets = np.random.randint(0, 30, size=len(dup))
        dup["effective_start_date"] = (pd.to_datetime(dup["effective_start_date"]) + pd.to_timedelta(offsets, unit="D")).dt.floor("D")
        eam = pd.concat([eam, dup], ignore_index=True)
    else:
        eam = eam.sample(n=target, random_state=SEED).reset_index(drop=True)

    # Non-critical nulls: effective_end_date mostly null already; keep inverter_model null for turbines
    return eam.reset_index(drop=True), eam[["plant_id", "equipment_id", "equipment_type", "inverter_model", "asset_type", "state", "region"]].drop_duplicates().reset_index(drop=True)


def generate_vendor_firmware_changes_tickets(eam_dim: pd.DataFrame, row_count_target: int) -> pd.DataFrame:
    print(f"Generating vendor_firmware_changes_tickets (~{row_count_target:,})...")

    target = int(max(1, int(row_count_target * SCALE_FACTOR)))

    # Base background tickets (firmware releases etc.)
    n_background = max(0, target - 30)

    plant_ids = np.array([p["plant_id"] for p in PLANTS], dtype=object)
    plant_probs = _normalize_probs(
        np.array([0.16 if pid in AFFECTED_PLANTS else 0.08 for pid in plant_ids], dtype=float)
    )

    created_days = pd.date_range(pd.Timestamp("2025-10-01"), RANGE_END, freq="D")
    bg_days = np.random.choice(created_days, size=int(n_background), replace=True)

    ticket_type = _choose(
        np.array(["firmware_release", "rollout_change", "incident", "hotfix"], dtype=object),
        [0.62, 0.12, 0.22, 0.04],
        n_background,
    )

    bg_plant = np.random.choice(plant_ids, size=int(n_background), p=plant_probs)

    # equipment scope: only some tickets are equipment-specific
    eq_ids = eam_dim["equipment_id"].values
    inv_ids = eam_dim.loc[eam_dim["equipment_type"] == "inverter", "equipment_id"].values
    eq_scope = np.where(np.random.rand(n_background) < 0.35, np.random.choice(eq_ids, size=n_background), None)

    # model
    model = np.where(np.random.rand(n_background) < 0.75, "XG-440", _choose(np.array(["XG-360", "Other"], dtype=object), [0.7, 0.3], n_background))

    from_ver = np.where(model == "XG-440", "v3.14.2", "v2.9.0")
    to_ver = np.where(model == "XG-440", "v3.15.0", "v2.9.1")

    # status
    status = _choose(np.array(["planned", "in_progress", "completed", "mitigated", "rolled_back"], dtype=object), [0.22, 0.10, 0.58, 0.07, 0.03], n_background)

    # timestamps
    created_ts = pd.to_datetime(bg_days) + pd.to_timedelta(np.random.randint(0, 24, size=n_background), unit="h")
    start_ts = created_ts + pd.to_timedelta(np.random.randint(0, 72, size=n_background), unit="h")
    end_ts = start_ts + pd.to_timedelta(np.random.randint(0, 36, size=n_background), unit="h")

    change_reason = _choose(
        np.array(["performance", "cybersecurity", "stability", "bugfix", "compliance", "other"], dtype=object),
        [0.24, 0.10, 0.20, 0.30, 0.08, 0.08],
        n_background,
    )

    summaries = []
    for i in range(int(n_background)):
        code = np.random.choice(["TRIP-31", "TRIP-44", "DERATE-12", "COMMS-01", "GRID-08", "OTHER-00"])
        summaries.append(f"Routine vendor ticket; observed {code}; review settings and monitor.")

    bg = pd.DataFrame(
        {
            "ticket_id": [f"VND-CHG-{i + 10000:05d}" for i in range(int(n_background))],
            "ticket_type": ticket_type,
            "created_ts_utc": created_ts,
            "change_start_ts_utc": start_ts,
            "change_end_ts_utc": end_ts,
            "plant_id": bg_plant,
            "equipment_id": eq_scope,
            "equipment_type": np.where(np.isin(eq_scope, inv_ids), "inverter", "inverter"),
            "inverter_model": model,
            "from_firmware_version": from_ver,
            "to_firmware_version": to_ver,
            "vendor": np.array(["XG_Power"] * n_background, dtype=object),
            "change_reason": change_reason,
            "status": status,
            "summary": summaries,
        }
    )

    # Inject the story-critical tickets: rollout for 3 affected plants and hotfix
    critical_rows = []
    seq = 20000
    for pid in AFFECTED_PLANTS:
        for _ in range(6):
            critical_rows.append(
                {
                    "ticket_id": f"VND-CHG-{seq:05d}",
                    "ticket_type": "rollout_change",
                    "created_ts_utc": ROLLOUT_START + pd.Timedelta(hours=int(np.random.randint(7, 12))),
                    "change_start_ts_utc": ROLLOUT_START + pd.Timedelta(hours=7),
                    "change_end_ts_utc": ROLLOUT_END + pd.Timedelta(hours=18),
                    "plant_id": pid,
                    "equipment_id": None,
                    "equipment_type": "inverter",
                    "inverter_model": "XG-440",
                    "from_firmware_version": "v3.14.2",
                    "to_firmware_version": "v3.15.0",
                    "vendor": "XG_Power",
                    "change_reason": np.random.choice(["performance", "bugfix"]),
                    "status": "completed",
                    "summary": "Firmware rollout XG-440 v3.15.0; scope plant fleet. Monitor TRIP-31/TRIP-44 rates post-change.",
                }
            )
            seq += 1

        # incident tickets shortly after anomaly start
        for _ in range(3):
            critical_rows.append(
                {
                    "ticket_id": f"VND-CHG-{seq:05d}",
                    "ticket_type": "incident",
                    "created_ts_utc": ANOMALY_START + pd.Timedelta(hours=int(np.random.randint(8, 16))),
                    "change_start_ts_utc": ANOMALY_START + pd.Timedelta(hours=8),
                    "change_end_ts_utc": ANOMALY_START + pd.Timedelta(hours=20),
                    "plant_id": pid,
                    "equipment_id": None,
                    "equipment_type": "inverter",
                    "inverter_model": "XG-440",
                    "from_firmware_version": "v3.15.0",
                    "to_firmware_version": "v3.15.0",
                    "vendor": "XG_Power",
                    "change_reason": "stability",
                    "status": "mitigated",
                    "summary": "Incident: increased nuisance trips after rollout; TRIP-31/TRIP-44 elevated; evaluate rollback thresholds.",
                }
            )
            seq += 1

    # hotfix ticket on 2025-12-08 for affected plants
    for pid in AFFECTED_PLANTS:
        critical_rows.append(
            {
                "ticket_id": f"VND-CHG-{seq:05d}",
                "ticket_type": "hotfix",
                "created_ts_utc": HOTFIX_DATE + pd.Timedelta(hours=7),
                "change_start_ts_utc": HOTFIX_DATE + pd.Timedelta(hours=8),
                "change_end_ts_utc": HOTFIX_DATE + pd.Timedelta(hours=18),
                "plant_id": pid,
                "equipment_id": None,
                "equipment_type": "inverter",
                "inverter_model": "XG-440",
                "from_firmware_version": "v3.15.0",
                "to_firmware_version": "v3.15.1-hotfix",
                "vendor": "XG_Power",
                "change_reason": "stability",
                "status": "completed",
                "summary": "Hotfix applied to reduce nuisance protective trips and derate behavior under grid constraints.",
            }
        )
        seq += 1

    critical = pd.DataFrame(critical_rows)

    df = pd.concat([bg, critical], ignore_index=True)

    # trim / pad to target
    if len(df) > target:
        df = df.sample(n=target, random_state=SEED).reset_index(drop=True)
    elif len(df) < target:
        extra = target - len(df)
        dup = df.sample(n=extra, replace=True, random_state=SEED).copy()
        # adjust ticket_id for uniqueness
        dup["ticket_id"] = [f"VND-CHG-{i + 90000:05d}" for i in range(extra)]
        df = pd.concat([df, dup], ignore_index=True)

    # tiny null rate in equipment_id already; keep.
    return df


def generate_scada_telemetry_events(eam_dim: pd.DataFrame, row_count_target: int) -> pd.DataFrame:
    print(f"Generating scada_telemetry_events (~{row_count_target:,})...")

    target = int(max(1, int(row_count_target * SCALE_FACTOR)))

    # We'll generate at 15-min granularity per plant, then scale row count with sparse event rows.
    base_intervals = pd.date_range(RANGE_START, RANGE_END + pd.Timedelta(days=1), freq="15min", inclusive="left")

    plant_df = pd.DataFrame(PLANTS)

    # Precompute day factors: weekday-heavy curtailment after anomaly for affected plants
    day_df = pd.DataFrame({"settlement_day": DAYS})
    day_df["dow"] = day_df["settlement_day"].dt.dayofweek
    day_df["phase"] = day_df["settlement_day"].apply(_day_phase)
    day_df["is_weekday"] = (day_df["dow"] < 5).astype(int)

    # baseline solar slightly higher on weekends (maintenance scheduling) and curtailment more common on weekdays
    day_df["solar_day_mul"] = np.where(day_df["dow"] >= 5, 1.04, 0.98)
    day_df["wind_day_mul"] = np.where(day_df["dow"] >= 5, 1.00, 1.00)

    # Curtailment minutes expectation per plant-day
    base_curtail = 18.0  # minutes/day per equipment sample proxy
    day_df["curtail_base_mul"] = np.where(day_df["is_weekday"] == 1, 1.35, 0.75)

    # Plant-level: affected TX solar has stacked curtailment after anomaly (weekday-heavy)
    def curtail_multiplier(phase: str, is_affected: bool) -> float:
        if not is_affected:
            return 1.0
        if phase == "baseline":
            return 1.0
        if phase == "impact":
            return 2.4
        return 1.6  # post-hotfix still elevated but improved

    # Map equipment to plant and model
    equip = eam_dim[["plant_id", "equipment_id", "equipment_type", "inverter_model", "asset_type", "state", "region"]].drop_duplicates().copy()

    # sample interval rows per plant (not per equipment) then assign equipment ids with weighting
    # We'll generate approx intervals_per_day_per_plant rows and then augment with alarm events to hit target.
    intervals_per_day = 96  # 15-min
    n_plants = len(PLANTS)
    base_rows = int(len(DAYS) * intervals_per_day * n_plants)

    # Determine how many base rows we can keep relative to target
    # Keep at most 85% as base telemetry rows; remainder are alarm/event rows.
    base_keep = int(min(base_rows, int(target * 0.85)))
    event_rows = int(max(0, target - base_keep))

    # Sample base_keep intervals uniformly across plant-interval combinations
    # Create indices: choose (plant_idx, interval_idx)
    total_combos = len(base_intervals) * n_plants
    combo_idx = np.random.choice(np.arange(total_combos), size=base_keep, replace=False if base_keep < total_combos else True)
    plant_idx = (combo_idx // len(base_intervals)).astype(int)
    interval_idx = (combo_idx % len(base_intervals)).astype(int)

    plant_ids = plant_df.loc[plant_idx, "plant_id"].values
    asset_type = plant_df.loc[plant_idx, "asset_type"].values
    state = plant_df.loc[plant_idx, "state"].values
    region = plant_df.loc[plant_idx, "region"].values
    affected = plant_df.loc[plant_idx, "affected"].values.astype(bool)

    event_ts = base_intervals[interval_idx]
    settlement_day = pd.to_datetime(event_ts).floor("D")

    # Join day multipliers
    day_map = day_df.set_index("settlement_day")
    dow = day_map.loc[settlement_day, "dow"].to_numpy()
    phase = day_map.loc[settlement_day, "phase"].to_numpy()

    # Per-row baseline MWh per plant-day, then allocate to interval using solar profile or uniform wind
    baseline_daily = np.array([BASELINE_MWH_BY_PLANT[pid] for pid in plant_ids], dtype=float)
    post_daily = np.array([POST_EVENT_MWH_BY_PLANT[pid] for pid in plant_ids], dtype=float)
    hotfix_daily = np.array([HOTFIX_MWH_BY_PLANT[pid] for pid in plant_ids], dtype=float)

    daily_target = baseline_daily.copy()
    impact_mask = (pd.to_datetime(settlement_day) >= ANOMALY_START) & (pd.to_datetime(settlement_day) < HOTFIX_DATE)
    post_mask = pd.to_datetime(settlement_day) >= HOTFIX_DATE
    daily_target[impact_mask] = post_daily[impact_mask]
    daily_target[post_mask] = hotfix_daily[post_mask]

    # Add small weather noise (not flat)
    daily_target *= np.clip(np.random.normal(1.0, 0.03, size=base_keep), 0.90, 1.12)

    # Interval allocation
    hours = pd.to_datetime(event_ts).hour.to_numpy()
    solar_prof = _solar_daylight_profile(hours)
    # normalize solar profile per day for solar rows
    is_solar = asset_type == "solar"

    # For solar rows, approximate interval share as profile / sum(profile per day) * daily_target
    # Precompute day-hour sums for the solar profile
    # Build per-day total profile weight for each timestamp
    ts_day = settlement_day
    solar_weight = solar_prof.copy()
    solar_weight[~is_solar] = 1.0

    # total profile per day for solar rows (avoid groupby overhead with factorization)
    day_codes, day_uniques = pd.factorize(ts_day)
    prof_sum = np.bincount(day_codes, weights=np.where(is_solar, solar_weight, 1.0))
    prof_sum = np.clip(prof_sum, 1e-6, None)
    interval_share = solar_weight / prof_sum[day_codes]

    net_mwh = daily_target * interval_share

    # Availability + status driven by story
    status = np.empty(base_keep, dtype=object)
    availability_flag = np.ones(base_keep, dtype=bool)
    downtime_minutes = np.zeros(base_keep, dtype=np.int32)
    curtailment_flag = np.zeros(base_keep, dtype=bool)
    curtailment_window_minutes = np.zeros(base_keep, dtype=np.int32)
    alarm_code = np.array([None] * base_keep, dtype=object)
    firmware_version = np.array([None] * base_keep, dtype=object)

    # Assign equipment_id by sampling from plant equipment pools (vectorized via mapping)
    equip_by_plant = {pid: equip.loc[equip["plant_id"] == pid] for pid in equip["plant_id"].unique()}

    equipment_id = np.empty(base_keep, dtype=object)
    equipment_type = np.empty(base_keep, dtype=object)
    inverter_model = np.array([None] * base_keep, dtype=object)

    for pid in np.unique(plant_ids):
        idx = np.where(plant_ids == pid)[0]
        pool = equip_by_plant[pid]
        # power-law weighting across equipment: a few devices generate more telemetry rows
        n_pool = len(pool)
        ranks = np.arange(1, n_pool + 1)
        w = 1.0 / np.power(ranks, 0.9)
        w = _normalize_probs(w)
        pick = np.random.choice(np.arange(n_pool), size=len(idx), replace=True, p=w)
        chosen = pool.iloc[pick]
        equipment_id[idx] = chosen["equipment_id"].values
        equipment_type[idx] = chosen["equipment_type"].values
        inverter_model[idx] = chosen["inverter_model"].values

    # Firmware: for XG-440 in affected plants, switch versions around rollout and hotfix
    is_xg440 = (inverter_model == "XG-440")
    in_affected_plant = np.isin(plant_ids, np.array(AFFECTED_PLANTS, dtype=object))
    xg_scope = is_xg440 & in_affected_plant

    day_ts = pd.to_datetime(settlement_day)
    pre_roll = day_ts < ROLLOUT_START
    post_roll = (day_ts >= ROLLOUT_END)
    post_hotfix = day_ts >= HOTFIX_DATE

    firmware_version[xg_scope & pre_roll] = "v3.14.2"
    firmware_version[xg_scope & post_roll & (~post_hotfix)] = "v3.15.0"
    firmware_version[xg_scope & post_hotfix] = "v3.15.1-hotfix"

    # For other inverters, sporadic firmware strings or null
    other_inv = (equipment_type == "inverter") & (~xg_scope)
    fill_other = np.random.rand(base_keep) < 0.40
    firmware_version[other_inv & fill_other] = _choose(np.array(["v2.9.0", "v2.9.1", "v3.10.0"], dtype=object), [0.4, 0.35, 0.25], int((other_inv & fill_other).sum()))

    # Status probabilities
    # Baseline: mostly online; impact: xg440 in affected plants has higher tripped/derated; post-hotfix partial improvement
    base_probs = np.array([0.955, 0.010, 0.020, 0.006, 0.007, 0.002])
    impact_probs_xg = np.array([0.875, 0.020, 0.060, 0.030, 0.012, 0.003])
    post_probs_xg = np.array([0.915, 0.015, 0.040, 0.018, 0.010, 0.002])

    status[:] = _choose(STATUS_VALUES, base_probs, base_keep)

    # overwrite for xg scope during impact/post
    if xg_scope.any():
        imp = xg_scope & impact_mask
        pst = xg_scope & post_mask
        if imp.any():
            status[imp] = _choose(STATUS_VALUES, impact_probs_xg, int(imp.sum()))
        if pst.any():
            status[pst] = _choose(STATUS_VALUES, post_probs_xg, int(pst.sum()))

    # availability_flag mapping
    availability_flag = np.isin(status, np.array(["online", "derated"], dtype=object))

    # downtime minutes for non-available rows (mostly 0 for telemetry; small for offline/tripped)
    down_mask = ~availability_flag
    if down_mask.any():
        downtime_minutes[down_mask] = np.random.randint(5, 60, size=int(down_mask.sum())).astype(np.int32)

    # Curtailment: plant-day, weekday-heavy after anomaly for affected plants
    # We assign curtailment flags and minutes to a subset of rows; later gold sums minutes by day/plant
    curtail_base = base_curtail * day_map.loc[settlement_day, "curtail_base_mul"].to_numpy()
    curtail_mult = np.array([curtail_multiplier(ph, aff) for ph, aff in zip(phase, affected)], dtype=float)
    curtail_expected = curtail_base * curtail_mult

    # Convert expected minutes/day proxy to per-row curtailment chance
    # Higher in solar daylight intervals
    curtail_chance = np.clip((curtail_expected / 240.0) * (0.6 + 0.8 * solar_prof), 0.0, 0.55)
    curtailment_flag = np.random.rand(base_keep) < curtail_chance
    curtailment_window_minutes[curtailment_flag] = np.random.randint(10, 90, size=int(curtailment_flag.sum())).astype(np.int32)

    # Alarm codes: for tripped/derated statuses, assign codes; else mostly null
    alarm_mask = np.isin(status, np.array(["tripped", "derated", "comms_lost"], dtype=object))
    if alarm_mask.any():
        # Increase TRIP codes share for xg scope post-event
        trip_codes = np.array(["TRIP-31", "TRIP-44", "TRIP-12"], dtype=object)
        derate_codes = np.array(["DERATE-12", "DERATE-09", "DERATE-21"], dtype=object)
        comms_codes = np.array(["COMMS-01", "COMMS-07"], dtype=object)
        other_codes = np.array(["GRID-02", "GRID-08", "TEMP-05", "VOLT-03", "FREQ-02", "OTHER-00"], dtype=object)

        idx = np.where(alarm_mask)[0]
        st = status[idx]
        is_trip = st == "tripped"
        is_der = st == "derated"
        is_com = st == "comms_lost"

        # trip codes
        if is_trip.any():
            ix = idx[is_trip]
            # impacted xg has heavier TRIP-31/TRIP-44
            xg_ix = xg_scope[ix]
            alarm_code[ix[xg_ix]] = _choose(trip_codes, [0.56, 0.30, 0.14], int(xg_ix.sum()))
            alarm_code[ix[~xg_ix]] = _choose(trip_codes, [0.40, 0.22, 0.38], int((~xg_ix).sum()))
        if is_der.any():
            ix = idx[is_der]
            xg_ix = xg_scope[ix]
            alarm_code[ix[xg_ix]] = _choose(derate_codes, [0.55, 0.25, 0.20], int(xg_ix.sum()))
            alarm_code[ix[~xg_ix]] = _choose(derate_codes, [0.40, 0.35, 0.25], int((~xg_ix).sum()))
        if is_com.any():
            ix = idx[is_com]
            alarm_code[ix] = _choose(comms_codes, [0.7, 0.3], len(ix))

        # remaining (rare)
        rem = alarm_mask & (alarm_code == None)
        if rem.any():
            alarm_code[rem] = _choose(other_codes, [0.18, 0.16, 0.18, 0.16, 0.12, 0.20], int(rem.sum()))

    # net_mwh should be non-negative; reduce if unavailable and/or curtailed
    # unavailability knocks down production (solar only strongly)
    net_mwh = np.clip(net_mwh, 0.0, None)
    if (~availability_flag).any():
        # Apply stronger production loss for solar when unavailable; keep shapes aligned
        loss = np.where(is_solar, 0.85, 0.55).astype(float)
        net_mwh[~availability_flag] *= (1.0 - loss[~availability_flag])

    if curtailment_flag.any():
        net_mwh[curtailment_flag] *= np.random.uniform(0.70, 0.92, size=int(curtailment_flag.sum()))

    # Create base telemetry dataframe
    df_base = pd.DataFrame(
        {
            "event_id": [str(uuid.uuid4()) for _ in range(base_keep)],
            "event_ts_utc": pd.to_datetime(event_ts),
            "settlement_day": pd.to_datetime(settlement_day).floor("D"),
            "plant_id": plant_ids,
            "state": state,
            "region": region,
            "asset_type": asset_type,
            "equipment_id": equipment_id,
            "equipment_type": equipment_type,
            "inverter_model": inverter_model,
            "firmware_version": firmware_version,
            "status": status,
            "net_mwh": np.round(net_mwh.astype(float), 6),
            "availability_flag": availability_flag.astype(bool),
            "downtime_minutes": downtime_minutes.astype(np.int32),
            "curtailment_flag": curtailment_flag.astype(bool),
            "curtailment_window_minutes": curtailment_window_minutes.astype(np.int32),
            "alarm_code": alarm_code,
        }
    )

    # Add explicit alarm/event rows to reach target (higher in impact for affected plants)
    if event_rows > 0:
        # sample days with heavier probability during impact window for affected plants
        day_weights = np.ones(len(DAYS), dtype=float)
        day_weights[DAYS >= ANOMALY_START] *= 1.6
        day_weights[(DAYS >= ANOMALY_START) & (DAYS < HOTFIX_DATE)] *= 2.2
        day_weights[DAYS >= HOTFIX_DATE] *= 1.4
        day_weights = _normalize_probs(day_weights)

        ev_days = np.random.choice(DAYS, size=int(event_rows), p=day_weights)
        ev_is_impact = (ev_days >= ANOMALY_START) & (ev_days < HOTFIX_DATE)
        ev_is_post = ev_days >= HOTFIX_DATE

        # plant selection: impacted days skew to affected plants
        plant_probs_base = np.array([0.10 if p["asset_type"] == "wind" else 0.13 for p in PLANTS], dtype=float)
        plant_probs_imp = np.array([0.22 if p["plant_id"] in AFFECTED_PLANTS else 0.08 for p in PLANTS], dtype=float)
        plant_probs_post = np.array([0.18 if p["plant_id"] in AFFECTED_PLANTS else 0.09 for p in PLANTS], dtype=float)
        plant_probs_base = _normalize_probs(plant_probs_base)
        plant_probs_imp = _normalize_probs(plant_probs_imp)
        plant_probs_post = _normalize_probs(plant_probs_post)

        ev_plant = np.empty(event_rows, dtype=object)
        base_mask = ~(ev_is_impact | ev_is_post)
        if base_mask.any():
            ev_plant[base_mask] = np.random.choice(plant_df["plant_id"].values, size=int(base_mask.sum()), p=plant_probs_base)
        if ev_is_impact.any():
            ev_plant[ev_is_impact] = np.random.choice(plant_df["plant_id"].values, size=int(ev_is_impact.sum()), p=plant_probs_imp)
        if ev_is_post.any():
            ev_plant[ev_is_post] = np.random.choice(plant_df["plant_id"].values, size=int(ev_is_post.sum()), p=plant_probs_post)

        # timestamps within day
        ev_ts = pd.to_datetime(ev_days) + pd.to_timedelta(np.random.randint(0, 24, size=event_rows), unit="h") + pd.to_timedelta(np.random.randint(0, 60, size=event_rows), unit="m")

        # choose equipment scoped to plant, more likely XG-440 in affected plants
        ev_equipment_id = np.empty(event_rows, dtype=object)
        ev_equipment_type = np.empty(event_rows, dtype=object)
        ev_inverter_model = np.array([None] * event_rows, dtype=object)

        for pid in np.unique(ev_plant):
            ix = np.where(ev_plant == pid)[0]
            pool = equip_by_plant[pid]
            if len(pool) == 0:
                continue
            if pid in AFFECTED_PLANTS:
                # weight to XG-440
                m = pool["inverter_model"].fillna("NA").values
                w = np.where(m == "XG-440", 5.0, 1.0)
                w = _normalize_probs(w)
                pick = np.random.choice(np.arange(len(pool)), size=len(ix), replace=True, p=w)
            else:
                pick = np.random.choice(np.arange(len(pool)), size=len(ix), replace=True)
            chosen = pool.iloc[pick]
            ev_equipment_id[ix] = chosen["equipment_id"].values
            ev_equipment_type[ix] = chosen["equipment_type"].values
            ev_inverter_model[ix] = chosen["inverter_model"].values

        # state/region/asset_type from plant
        plant_lookup = plant_df.set_index("plant_id")
        ev_state = plant_lookup.loc[ev_plant, "state"].to_numpy()
        ev_region = plant_lookup.loc[ev_plant, "region"].to_numpy()
        ev_asset_type = plant_lookup.loc[ev_plant, "asset_type"].to_numpy()

        ev_settle = pd.to_datetime(ev_ts).floor("D")
        ev_day_ts = pd.to_datetime(ev_settle)

        # firmware versions
        ev_fw = np.array([None] * event_rows, dtype=object)
        ev_xg_scope = (ev_inverter_model == "XG-440") & np.isin(ev_plant, np.array(AFFECTED_PLANTS, dtype=object))
        ev_fw[ev_xg_scope & (ev_day_ts < ROLLOUT_START)] = "v3.14.2"
        ev_fw[ev_xg_scope & (ev_day_ts >= ROLLOUT_END) & (ev_day_ts < HOTFIX_DATE)] = "v3.15.0"
        ev_fw[ev_xg_scope & (ev_day_ts >= HOTFIX_DATE)] = "v3.15.1-hotfix"

        # statuses biased to non-normal for event rows
        ev_status = np.empty(event_rows, dtype=object)
        ev_status[:] = _choose(STATUS_VALUES, [0.45, 0.08, 0.18, 0.18, 0.07, 0.04], event_rows)
        # impacted xg even more tripped/derated
        if ev_xg_scope.any():
            ix = np.where(ev_xg_scope & (ev_day_ts >= ANOMALY_START) & (ev_day_ts < HOTFIX_DATE))[0]
            if len(ix) > 0:
                ev_status[ix] = _choose(STATUS_VALUES, [0.28, 0.10, 0.24, 0.26, 0.08, 0.04], len(ix))

        ev_avail = np.isin(ev_status, np.array(["online", "derated"], dtype=object))
        ev_down = np.zeros(event_rows, dtype=np.int32)
        ev_down[~ev_avail] = np.random.randint(10, 180, size=int((~ev_avail).sum())).astype(np.int32)

        # curtailment minutes: higher after anomaly on weekdays for affected plants
        ev_dow = ev_day_ts.dayofweek.to_numpy()
        weekday = ev_dow < 5
        ev_curt_flag = np.zeros(event_rows, dtype=bool)
        ev_curt_min = np.zeros(event_rows, dtype=np.int32)
        aff = np.isin(ev_plant, np.array(AFFECTED_PLANTS, dtype=object)) & (ev_asset_type == "solar")
        base_c = 0.08 + 0.06 * weekday
        imp_c = 0.22 + 0.12 * weekday
        post_c = 0.14 + 0.08 * weekday
        p_c = base_c.copy()
        p_c[(ev_day_ts >= ANOMALY_START) & (ev_day_ts < HOTFIX_DATE)] = imp_c[(ev_day_ts >= ANOMALY_START) & (ev_day_ts < HOTFIX_DATE)]
        p_c[(ev_day_ts >= HOTFIX_DATE)] = post_c[(ev_day_ts >= HOTFIX_DATE)]
        p_c = np.clip(p_c + aff.astype(float) * 0.10, 0.0, 0.55)
        ev_curt_flag = np.random.rand(event_rows) < p_c
        ev_curt_min[ev_curt_flag] = np.random.randint(20, 160, size=int(ev_curt_flag.sum())).astype(np.int32)

        # alarm codes more present
        ev_alarm = _choose(ALARM_CODES, [0.12, 0.10, 0.06, 0.10, 0.06, 0.05, 0.08, 0.04, 0.03, 0.08, 0.06, 0.06, 0.05, 0.04, 0.07], event_rows)
        # force xg scope to trip-heavy
        if ev_xg_scope.any():
            ix = np.where(ev_xg_scope & (ev_day_ts >= ANOMALY_START) & (ev_day_ts < HOTFIX_DATE))[0]
            if len(ix) > 0:
                ev_alarm[ix] = _choose(np.array(["TRIP-31", "TRIP-44", "DERATE-12", "DERATE-21"], dtype=object), [0.46, 0.26, 0.18, 0.10], len(ix))

        # net_mwh small in event rows; keep non-negative
        # Use small interval energy with heavy reduction if non-available/curtailed
        ev_net = np.zeros(event_rows, dtype=float)
        # base per-row energy estimate from plant daily / (96* equipment_per_plant?) - small
        ev_daily = np.array([BASELINE_MWH_BY_PLANT[pid] for pid in ev_plant], dtype=float)
        ev_net = (ev_daily / 96.0) * np.random.uniform(0.15, 0.35, size=event_rows)
        ev_net[~ev_avail] *= np.random.uniform(0.05, 0.25, size=int((~ev_avail).sum()))
        ev_net[ev_curt_flag] *= np.random.uniform(0.40, 0.80, size=int(ev_curt_flag.sum()))
        ev_net = np.clip(ev_net, 0.0, None)

        df_ev = pd.DataFrame(
            {
                "event_id": [str(uuid.uuid4()) for _ in range(event_rows)],
                "event_ts_utc": pd.to_datetime(ev_ts),
                "settlement_day": pd.to_datetime(ev_settle).floor("D"),
                "plant_id": ev_plant,
                "state": ev_state,
                "region": ev_region,
                "asset_type": ev_asset_type,
                "equipment_id": ev_equipment_id,
                "equipment_type": ev_equipment_type,
                "inverter_model": ev_inverter_model,
                "firmware_version": ev_fw,
                "status": ev_status,
                "net_mwh": np.round(ev_net.astype(float), 6),
                "availability_flag": ev_avail.astype(bool),
                "downtime_minutes": ev_down.astype(np.int32),
                "curtailment_flag": ev_curt_flag.astype(bool),
                "curtailment_window_minutes": ev_curt_min.astype(np.int32),
                "alarm_code": ev_alarm,
            }
        )

        df = pd.concat([df_base, df_ev], ignore_index=True)
    else:
        df = df_base

    # tiny null rate in non-critical firmware_version and alarm_code already
    return df.sample(frac=1.0, random_state=SEED).reset_index(drop=True)


def generate_cmms_work_orders_dispatch(eam_dim: pd.DataFrame, vendor_tickets: pd.DataFrame, row_count_target: int) -> pd.DataFrame:
    print(f"Generating cmms_work_orders_dispatch (~{row_count_target:,})...")

    target = int(max(1, int(row_count_target * SCALE_FACTOR)))

    # Create base daily volumes and amplify corrective post-anomaly in affected plants
    n_days = len(DAYS)

    # baseline ~340 rows/day overall at full scale -> 340*62 ~ 21k; with dispatch lines 1-3 -> 28k-ish
    baseline_per_day = max(40, int(340 * SCALE_FACTOR))

    # Build day weights: weekday-heavy dispatch; higher post-anomaly
    dow = DAYS.dayofweek.to_numpy()
    day_weight = np.where(dow < 5, 1.10, 0.65).astype(float)
    day_weight[DAYS >= ANOMALY_START] *= 1.35
    day_weight[(DAYS >= ANOMALY_START) & (DAYS < HOTFIX_DATE)] *= 1.55
    day_weight[DAYS >= HOTFIX_DATE] *= 1.20
    day_weight = _normalize_probs(day_weight)

    created_days = np.random.choice(DAYS, size=target, p=day_weight)

    # Plant selection: impacted days skew to affected plants for solar
    plant_ids = np.array([p["plant_id"] for p in PLANTS], dtype=object)
    base_probs = _normalize_probs(np.array([0.11 if p["asset_type"] == "wind" else 0.14 for p in PLANTS], dtype=float))
    imp_probs = _normalize_probs(np.array([0.24 if p["plant_id"] in AFFECTED_PLANTS else 0.07 for p in PLANTS], dtype=float))

    is_imp = created_days >= ANOMALY_START
    plant_pick = np.empty(target, dtype=object)
    plant_pick[~is_imp] = np.random.choice(plant_ids, size=int((~is_imp).sum()), p=base_probs)
    plant_pick[is_imp] = np.random.choice(plant_ids, size=int(is_imp.sum()), p=imp_probs)

    plant_lookup = pd.DataFrame(PLANTS).set_index("plant_id")
    asset_type = plant_lookup.loc[plant_pick, "asset_type"].to_numpy()

    # equipment per plant
    equip = eam_dim[["plant_id", "equipment_id", "equipment_type", "inverter_model"]].drop_duplicates().copy()
    equip_by_plant = {pid: equip.loc[equip["plant_id"] == pid] for pid in equip["plant_id"].unique()}

    equipment_id = np.empty(target, dtype=object)
    equipment_type = np.empty(target, dtype=object)

    for pid in np.unique(plant_pick):
        ix = np.where(plant_pick == pid)[0]
        pool = equip_by_plant[pid]
        if len(pool) == 0:
            continue
        # corrective work orders focus on inverters for affected plants
        pick = np.random.choice(np.arange(len(pool)), size=len(ix), replace=True)
        chosen = pool.iloc[pick]
        equipment_id[ix] = chosen["equipment_id"].values
        equipment_type[ix] = chosen["equipment_type"].values

    # Work order type: corrective increases post-event for affected plants
    wo_type = np.empty(target, dtype=object)
    base_corr = 0.38
    imp_corr = 0.62
    aff = np.isin(plant_pick, np.array(AFFECTED_PLANTS, dtype=object)) & (asset_type == "solar")
    p_corr = np.where(is_imp & aff, imp_corr, base_corr)
    wo_type[:] = np.where(np.random.rand(target) < p_corr, "corrective", "preventive")

    # Issue category
    issue = np.empty(target, dtype=object)
    # for corrective inverter work orders in affected plants post-event: inverter_trip and reactive_power dominate
    special = (wo_type == "corrective") & aff & is_imp
    if special.any():
        issue[special] = _choose(ISSUE_CATEGORIES, [0.52, 0.08, 0.22, 0.04, 0.02, 0.02, 0.04, 0.06], int(special.sum()))
    rest = ~special
    if rest.any():
        issue[rest] = _choose(ISSUE_CATEGORIES, [0.20, 0.12, 0.10, 0.12, 0.10, 0.10, 0.16, 0.10], int(rest.sum()))

    # Vendor
    vendor = np.empty(target, dtype=object)
    if special.any():
        vendor[special] = _choose(VENDORS, [0.78, 0.02, 0.06, 0.10, 0.04], int(special.sum()))
    if rest.any():
        vendor[rest] = _choose(VENDORS, [0.35, 0.28, 0.10, 0.18, 0.09], int(rest.sum()))

    # labor hours and costs
    # labor hours right-skew
    lh = np.random.lognormal(mean=np.log(2.8), sigma=0.65, size=target)
    # corrective tends higher
    lh[wo_type == "corrective"] *= np.random.uniform(1.25, 1.85, size=int((wo_type == "corrective").sum()))
    # incident troubleshooting increases hours for special
    lh[special] *= np.random.uniform(1.20, 1.55, size=int(special.sum()))
    lh = np.clip(lh, 0.5, 24.0)

    rate = np.where(vendor == "XG_Power", 175.0, 145.0)  # simple rate by vendor
    labor_cost = lh * rate
    labor_cost = np.clip(labor_cost, 50.0, 6000.0)

    # parts cost: many zeros, occasional high values
    parts = np.random.lognormal(mean=np.log(220.0), sigma=1.25, size=target)
    zero_mask = np.random.rand(target) < 0.62
    parts[zero_mask] = 0.0
    # special incident has some controller swaps
    parts[special] *= np.random.uniform(1.10, 1.80, size=int(special.sum()))
    parts = np.clip(parts, 0.0, 25000.0)

    # timestamps
    created_ts = pd.to_datetime(created_days) + pd.to_timedelta(np.random.randint(0, 24, size=target), unit="h") + pd.to_timedelta(np.random.randint(0, 60, size=target), unit="m")
    created_date = pd.to_datetime(created_ts).floor("D")

    # completion: preventive more likely closed quickly; some open
    is_closed = np.random.rand(target) < np.where(wo_type == "preventive", 0.93, 0.82)
    cycle_hours = np.random.lognormal(mean=np.log(18.0), sigma=0.7, size=target)
    cycle_hours[wo_type == "preventive"] *= np.random.uniform(0.4, 0.9, size=int((wo_type == "preventive").sum()))
    cycle_hours[special] *= np.random.uniform(1.05, 1.55, size=int(special.sum()))
    cycle_hours = np.clip(cycle_hours, 1.0, 240.0)

    completed_ts = created_ts + pd.to_timedelta(cycle_hours, unit="h")
    completed_ts = pd.Series(completed_ts)
    completed_ts[~is_closed] = pd.NaT

    # dispatches: 1-3 per WO -> we store at dispatch row level already, so work_order_id repeats
    dispatches = np.random.choice([1, 2, 3], size=target, p=_normalize_probs([0.62, 0.28, 0.10]))
    # expand rows
    rep_idx = np.repeat(np.arange(target), dispatches)

    work_order_id = np.array([f"WO-{i + 1:07d}" for i in rep_idx], dtype=object)
    dispatch_id = np.array([f"DSP-{i + 1:08d}" for i in range(len(rep_idx))], dtype=object)

    # Dispatch timestamp around created time, weekday-heavy
    base_dispatch = created_ts.to_numpy()[rep_idx]
    dispatch_ts = pd.to_datetime(base_dispatch) + pd.to_timedelta(np.random.randint(0, 12, size=len(rep_idx)), unit="h")

    # techs
    n_tech = int(max(60, int(90 * SCALE_FACTOR)))
    technician_ids = np.array([f"TECH-{i + 1:04d}" for i in range(n_tech)], dtype=object)
    technician_id = np.random.choice(technician_ids, size=len(rep_idx), replace=True)

    # vendor tickets: optional; higher fill for special corrective inverter WOs post-event
    vendor_ticket_id = np.array([None] * len(rep_idx), dtype=object)
    ticket_pool = vendor_tickets.loc[vendor_tickets["plant_id"].isin(AFFECTED_PLANTS), "ticket_id"].values
    if len(ticket_pool) > 0:
        esc_prob = np.where(special[rep_idx], 0.32, 0.06)
        esc = np.random.rand(len(rep_idx)) < esc_prob
        vendor_ticket_id[esc] = np.random.choice(ticket_pool, size=int(esc.sum()), replace=True)

    df = pd.DataFrame(
        {
            "work_order_id": work_order_id,
            "created_ts_utc": pd.to_datetime(created_ts.to_numpy()[rep_idx]),
            "completed_ts_utc": pd.to_datetime(completed_ts.to_numpy()[rep_idx]),
            "created_date": pd.to_datetime(created_date.to_numpy()[rep_idx]).floor("D"),
            "plant_id": plant_pick[rep_idx],
            "equipment_id": equipment_id[rep_idx],
            "equipment_type": equipment_type[rep_idx],
            "work_order_type": wo_type[rep_idx],
            "issue_category": issue[rep_idx],
            "vendor": vendor[rep_idx],
            "labor_hours": np.round(lh[rep_idx].astype(float), 3),
            "parts_cost_usd": np.round(parts[rep_idx].astype(float), 2),
            "labor_cost_usd": np.round(labor_cost[rep_idx].astype(float), 2),
            "dispatch_id": dispatch_id,
            "technician_id": technician_id,
            "dispatch_ts_utc": pd.to_datetime(dispatch_ts),
            "vendor_ticket_id": vendor_ticket_id,
        }
    )

    # ensure final target-ish row count (can be > due to dispatch expansion); trim deterministically
    if len(df) > target:
        df = df.sample(n=target, random_state=SEED).reset_index(drop=True)

    return df


def generate_settlement_invoices_line_items(eam_master: pd.DataFrame, row_count_target: int) -> pd.DataFrame:
    print(f"Generating settlement_invoices_line_items (~{row_count_target:,})...")

    target = int(max(1, int(row_count_target * SCALE_FACTOR)))

    plants_df = pd.DataFrame(PLANTS)

    # We'll create ~line_items_per_day_per_plant rows, then sample to target.
    # Each (plant, day) has merchant_revenue plus 1-4 other charges.
    line_items = []

    # price process
    # ERCOT more volatile; SPP slightly lower/less volatile
    def day_price(day: pd.Timestamp, region_val: str) -> float:
        base = 38.0 if region_val == "SPP" else 44.0
        # weekday higher volatility
        vol = 14.0 if day.dayofweek < 5 else 10.0
        spike = 1.0
        if region_val == "ERCOT" and (day >= pd.Timestamp("2025-11-01")):
            spike += np.random.exponential(0.10)
        p = np.random.normal(base * spike, vol)
        return float(np.clip(p, 5.0, 180.0))

    # Baseline revenue target: tie to plant baseline MWh and typical price
    # We will later scale totals so that post-event cumulative (lost revenue + abs charges) approximates $1.42M.

    for pid in plants_df["plant_id"].values:
        p = plants_df.set_index("plant_id").loc[pid]
        region_val = p["region"]
        state_val = p["state"]
        asset_type = p["asset_type"]
        affected = bool(p["affected"])

        # invoice id per plant-month
        for day in DAYS:
            inv_month = pd.Timestamp(day.year, day.month, 1)
            invoice_id = f"INV-{pid}-{inv_month.strftime('%Y%m')}"

            price = day_price(day, region_val)

            # baseline quantity approximates baseline mwh/day with noise
            baseline_mwh = float(BASELINE_MWH_BY_PLANT[pid]) * np.clip(np.random.normal(1.0, 0.02), 0.92, 1.08)

            # actual quantity: degrade for affected plants post anomaly with partial recovery after hotfix
            if day < ANOMALY_START:
                qty_mwh = baseline_mwh * np.clip(np.random.normal(1.0, 0.025), 0.92, 1.10)
            elif day < HOTFIX_DATE:
                if affected and asset_type == "solar":
                    qty_mwh = float(POST_EVENT_MWH_BY_PLANT[pid]) * np.clip(np.random.normal(1.0, 0.03), 0.88, 1.06)
                else:
                    qty_mwh = float(POST_EVENT_MWH_BY_PLANT[pid]) * np.clip(np.random.normal(1.0, 0.02), 0.92, 1.06)
            else:
                if affected and asset_type == "solar":
                    qty_mwh = float(HOTFIX_MWH_BY_PLANT[pid]) * np.clip(np.random.normal(1.0, 0.03), 0.90, 1.08)
                else:
                    qty_mwh = float(POST_EVENT_MWH_BY_PLANT[pid]) * np.clip(np.random.normal(1.0, 0.02), 0.92, 1.06)

            qty_mwh = float(max(qty_mwh, 0.0))

            # baseline revenue (used for variance)
            # incorporate merchant exposure
            mexp = float(
                eam_master.loc[eam_master["plant_id"] == pid, "merchant_exposure_pct"].iloc[0]
                if (eam_master["plant_id"] == pid).any()
                else 0.7
            )
            baseline_rev = baseline_mwh * price * mexp

            # merchant revenue
            merchant_rev = qty_mwh * price * mexp

            # penalties/imbalance: usually modest; increase post-event for affected plants
            in_impact = day >= ANOMALY_START
            base_pen = -abs(np.random.normal(0.0, 2800.0))
            base_imb = -abs(np.random.normal(0.0, 2200.0))
            if in_impact and affected and asset_type == "solar":
                base_pen *= np.random.uniform(2.3, 3.3)
                base_imb *= np.random.uniform(2.0, 3.2)

            # occasional uplift/other
            uplift = np.random.normal(0.0, 900.0)

            # Reasons
            reasons = np.array(["forecast_error", "forced_outage", "curtailment", "telemetry_gap", "schedule_change", "other"], dtype=object)
            reason_probs_base = [0.36, 0.10, 0.18, 0.08, 0.12, 0.16]
            reason_probs_imp = [0.42, 0.22, 0.16, 0.06, 0.08, 0.06]
            imb_reason = _choose(reasons, reason_probs_imp if (in_impact and affected and asset_type == "solar") else reason_probs_base, 1)[0]

            # Add line items
            line_items.append(
                {
                    "invoice_id": invoice_id,
                    "invoice_line_id": str(uuid.uuid4()),
                    "settlement_day": pd.Timestamp(day.date()),
                    "plant_id": pid,
                    "state": state_val,
                    "asset_type": asset_type,
                    "charge_type": "merchant_revenue",
                    "quantity_mwh": float(np.round(qty_mwh, 4)),
                    "amount_usd": float(np.round(merchant_rev, 2)),
                    "market_price_usd_per_mwh": float(np.round(price, 4)),
                    "baseline_revenue_usd": float(np.round(baseline_rev, 2)),
                    "imbalance_charge_reason": "other",
                }
            )

            # Penalty line
            line_items.append(
                {
                    "invoice_id": invoice_id,
                    "invoice_line_id": str(uuid.uuid4()),
                    "settlement_day": pd.Timestamp(day.date()),
                    "plant_id": pid,
                    "state": state_val,
                    "asset_type": asset_type,
                    "charge_type": "penalty",
                    "quantity_mwh": float(np.round(max(qty_mwh * np.random.uniform(0.01, 0.06), 0.0), 4)),
                    "amount_usd": float(np.round(base_pen, 2)),
                    "market_price_usd_per_mwh": float(np.round(price, 4)),
                    "baseline_revenue_usd": float(np.round(0.0, 2)),
                    "imbalance_charge_reason": imb_reason,
                }
            )

            # imbalance line
            line_items.append(
                {
                    "invoice_id": invoice_id,
                    "invoice_line_id": str(uuid.uuid4()),
                    "settlement_day": pd.Timestamp(day.date()),
                    "plant_id": pid,
                    "state": state_val,
                    "asset_type": asset_type,
                    "charge_type": "imbalance_charge",
                    "quantity_mwh": float(np.round(max(qty_mwh * np.random.uniform(0.02, 0.10), 0.0), 4)),
                    "amount_usd": float(np.round(base_imb, 2)),
                    "market_price_usd_per_mwh": float(np.round(price, 4)),
                    "baseline_revenue_usd": float(np.round(0.0, 2)),
                    "imbalance_charge_reason": imb_reason,
                }
            )

            # uplift and other sporadically
            if np.random.rand() < 0.45:
                line_items.append(
                    {
                        "invoice_id": invoice_id,
                        "invoice_line_id": str(uuid.uuid4()),
                        "settlement_day": pd.Timestamp(day.date()),
                        "plant_id": pid,
                        "state": state_val,
                        "asset_type": asset_type,
                        "charge_type": "uplift",
                        "quantity_mwh": float(np.round(0.0, 4)),
                        "amount_usd": float(np.round(uplift, 2)),
                        "market_price_usd_per_mwh": float(np.round(price, 4)),
                        "baseline_revenue_usd": float(np.round(0.0, 2)),
                        "imbalance_charge_reason": "other",
                    }
                )
            if np.random.rand() < 0.22:
                line_items.append(
                    {
                        "invoice_id": invoice_id,
                        "invoice_line_id": str(uuid.uuid4()),
                        "settlement_day": pd.Timestamp(day.date()),
                        "plant_id": pid,
                        "state": state_val,
                        "asset_type": asset_type,
                        "charge_type": "other",
                        "quantity_mwh": float(np.round(0.0, 4)),
                        "amount_usd": float(np.round(np.random.normal(0.0, 600.0), 2)),
                        "market_price_usd_per_mwh": float(np.round(price, 4)),
                        "baseline_revenue_usd": float(np.round(0.0, 2)),
                        "imbalance_charge_reason": "other",
                    }
                )

    df = pd.DataFrame(line_items)

    # Sample to target rows (keep story distribution by date/plant/charge_type)
    if len(df) > target:
        df = df.sample(n=target, random_state=SEED).reset_index(drop=True)

    # Scale impact to reach ~1.42M cumulative (lost revenue + abs charges) through 2025-12-15 from 2025-11-21
    # We compute a rough impact on the sampled raw set and scale penalty/imbalance magnitudes to match.
    post = df["settlement_day"] >= pd.Timestamp(ANOMALY_START.date())
    is_aff = df["plant_id"].isin(AFFECTED_PLANTS) & (df["asset_type"] == "solar")

    # Baseline lost revenue is baseline_revenue_usd - merchant revenue line amount
    m = df["charge_type"] == "merchant_revenue"
    lost = (df.loc[m & post & is_aff, "baseline_revenue_usd"].to_numpy() - df.loc[m & post & is_aff, "amount_usd"].to_numpy())
    lost = np.clip(lost, 0.0, None)
    pen_abs = np.abs(df.loc[(df["charge_type"] == "penalty") & post & is_aff, "amount_usd"].to_numpy())
    imb_abs = np.abs(df.loc[(df["charge_type"] == "imbalance_charge") & post & is_aff, "amount_usd"].to_numpy())

    current = float(lost.sum() + pen_abs.sum() + imb_abs.sum())
    target_impact = 1_420_000.0 * float(SCALE_FACTOR)
    if current > 0:
        mult = target_impact / current
        # apply multiplier to penalty and imbalance and (optionally) lost revenue by adjusting merchant revenue on impacted merchant lines
        # keep baseline_revenue_usd unchanged.
        df.loc[(df["charge_type"] == "penalty") & post & is_aff, "amount_usd"] *= float(mult)
        df.loc[(df["charge_type"] == "imbalance_charge") & post & is_aff, "amount_usd"] *= float(mult)
        # adjust merchant revenue downward/upward to scale lost revenue; use a gentler factor to avoid unrealistic negative
        adj = np.sqrt(mult)
        df.loc[m & post & is_aff, "amount_usd"] *= float(1.0 / adj)

    # ensure sign conventions remain
    df.loc[df["charge_type"].isin(["penalty", "imbalance_charge"]), "amount_usd"] = -np.abs(df.loc[df["charge_type"].isin(["penalty", "imbalance_charge"]), "amount_usd"].to_numpy())
    df.loc[df["charge_type"] == "merchant_revenue", "amount_usd"] = np.abs(df.loc[df["charge_type"] == "merchant_revenue", "amount_usd"].to_numpy())

    return df.reset_index(drop=True)


def _validate_story(scada: pd.DataFrame, settlements: pd.DataFrame, cmms: pd.DataFrame, eam: pd.DataFrame, vendor: pd.DataFrame):
    print("\n" + "=" * 60)
    print("VALIDATION SUMMARY (story signals)")
    print("=" * 60)

    # 1) Portfolio daily net MWh from SCADA
    sc = scada.copy()
    sc["d"] = pd.to_datetime(sc["settlement_day"], errors="coerce").dt.tz_localize(None).dt.floor("D")
    daily = sc.groupby("d", as_index=False)["net_mwh"].sum()

    pre = daily.loc[daily["d"] < ANOMALY_START, "net_mwh"].mean()
    imp = daily.loc[(daily["d"] >= ANOMALY_START) & (daily["d"] < HOTFIX_DATE), "net_mwh"].mean()
    post = daily.loc[daily["d"] >= HOTFIX_DATE, "net_mwh"].mean()

    print(f"Portfolio net MWh/day mean - pre: {pre:,.0f} | impact: {imp:,.0f} | post-hotfix: {post:,.0f}")

    # 2) Plant-level variance focus
    plant_daily = sc.groupby(["d", "plant_id"], as_index=False)["net_mwh"].sum()
    base_pl = plant_daily.loc[plant_daily["d"] < ANOMALY_START].groupby("plant_id")["net_mwh"].mean()
    imp_pl = plant_daily.loc[(plant_daily["d"] >= ANOMALY_START) & (plant_daily["d"] < HOTFIX_DATE)].groupby("plant_id")["net_mwh"].mean()
    focus = pd.DataFrame({"base": base_pl, "impact": imp_pl}).fillna(0.0)
    focus["delta"] = focus["impact"] - focus["base"]
    focus = focus.sort_values("delta").head(5)
    print("\nTop plants by impact delta (most negative):")
    print(focus.round(1))

    # 3) Availability by equipment model
    inv = sc[(sc["equipment_type"] == "inverter") & (sc["inverter_model"].notna())].copy()
    inv["period"] = np.where(inv["d"] < ANOMALY_START, "pre", np.where(inv["d"] < HOTFIX_DATE, "impact", "post"))
    av = inv.groupby(["period", "inverter_model"], as_index=False)["availability_flag"].mean()
    av["availability_pct"] = (av["availability_flag"] * 100.0).round(2)
    print("\nInverter availability by model and period (%):")
    print(av[["period", "inverter_model", "availability_pct"]].sort_values(["inverter_model", "period"]))

    # 4) Weekday curtailment pattern after anomaly for affected plants
    tx_aff = sc[sc["plant_id"].isin(AFFECTED_PLANTS)].copy()
    tx_aff["dow"] = tx_aff["d"].dt.dayofweek
    tx_aff["is_weekday"] = tx_aff["dow"] < 5
    w = tx_aff.groupby(["is_weekday"], as_index=False)["curtailment_window_minutes"].sum()
    w["label"] = np.where(w["is_weekday"], "weekday", "weekend")
    print("\nCurtailment minutes sum in affected plants (all days, weekday vs weekend):")
    print(w[["label", "curtailment_window_minutes"]])

    # 5) Settlement impact approx
    st = settlements.copy()
    st["settlement_day"] = pd.to_datetime(st["settlement_day"], errors="coerce").dt.tz_localize(None).dt.floor("D")
    post = st["settlement_day"] >= pd.Timestamp(ANOMALY_START.date())
    aff = st["plant_id"].isin(AFFECTED_PLANTS) & (st["asset_type"] == "solar")
    m = st["charge_type"] == "merchant_revenue"
    lost = (st.loc[m & post & aff, "baseline_revenue_usd"].to_numpy() - st.loc[m & post & aff, "amount_usd"].to_numpy())
    lost = np.clip(lost, 0.0, None).sum()
    pen = np.abs(st.loc[(st["charge_type"] == "penalty") & post & aff, "amount_usd"].to_numpy()).sum()
    imb = np.abs(st.loc[(st["charge_type"] == "imbalance_charge") & post & aff, "amount_usd"].to_numpy()).sum()
    total = float(lost + pen + imb)
    print(f"\nSettlement impact (approx, affected solar post 2025-11-21): ${total:,.0f} (target ~${1_420_000 * SCALE_FACTOR:,.0f})")

    # 6) O&M incremental spend signal
    cm = cmms.copy()
    cm["d"] = pd.to_datetime(cm["created_date"], errors="coerce").dt.tz_localize(None).dt.floor("D")
    cm["om_cost"] = cm["labor_cost_usd"].astype(float) + cm["parts_cost_usd"].astype(float)
    aff_cm = cm[cm["plant_id"].isin(AFFECTED_PLANTS) & (cm["equipment_type"] == "inverter")].copy()
    pre_cost = aff_cm.loc[aff_cm["d"] < ANOMALY_START, "om_cost"].sum()
    post_cost = aff_cm.loc[(aff_cm["d"] >= ANOMALY_START) & (aff_cm["d"] <= IMPACT_END), "om_cost"].sum()
    print(f"O&M cost in affected inverter scope - pre window sum: ${pre_cost:,.0f} | post window sum: ${post_cost:,.0f}")

    # 7) Referential integrity quick checks
    sc_missing = scada.merge(eam[["equipment_id"]].drop_duplicates(), on="equipment_id", how="left", indicator=True)
    missing_cnt = int((sc_missing["_merge"] == "left_only").sum())
    print(f"\nJoin check: scada.equipment_id not in eam master: {missing_cnt:,} rows")
    wo_missing = cmms.merge(eam[["equipment_id"]].drop_duplicates(), on="equipment_id", how="left", indicator=True)
    wo_missing_cnt = int((wo_missing["_merge"] == "left_only").sum())
    print(f"Join check: cmms.equipment_id not in eam master: {wo_missing_cnt:,} rows")

    print("=" * 60 + "\n")


if __name__ == "__main__":
    print("Starting ReNew Capital Partners data generation (story-driven)...")
    print(f"Scale factor: {SCALE_FACTOR}")
    print("-" * 60)

    # 1) EAM master (backbone)
    eam_master, eam_dim = generate_eam_asset_contract_master(row_count_target=6132)
    save_to_parquet(eam_master, "eam_asset_contract_master", num_files=3)

    # 2) Vendor firmware tickets
    vendor = generate_vendor_firmware_changes_tickets(eam_dim, row_count_target=487)
    save_to_parquet(vendor, "vendor_firmware_changes_tickets", num_files=1)

    # 3) SCADA telemetry/events
    scada = generate_scada_telemetry_events(eam_dim, row_count_target=382941)
    save_to_parquet(scada, "scada_telemetry_events", num_files=8)

    # 4) CMMS work orders/dispatch
    cmms = generate_cmms_work_orders_dispatch(eam_dim, vendor, row_count_target=28437)
    save_to_parquet(cmms, "cmms_work_orders_dispatch", num_files=5)

    # 5) Settlement invoice lines
    settlements = generate_settlement_invoices_line_items(eam_master, row_count_target=116248)
    save_to_parquet(settlements, "settlement_invoices_line_items", num_files=6)

    _validate_story(scada, settlements, cmms, eam_master, vendor)

    print("Generation complete.")
