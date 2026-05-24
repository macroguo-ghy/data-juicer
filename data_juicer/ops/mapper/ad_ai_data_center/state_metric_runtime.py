from __future__ import annotations

import datetime
import re
from typing import Any


def extract_numeric_ids(value: Any) -> list[str]:
    s = str(value or "").strip()
    if not s:
        return []
    if s.isdigit():
        return [s]
    out = []
    seen = set()
    for item in re.findall(r"\d+", s):
        if item in seen:
            continue
        seen.add(item)
        out.append(item)
    return out


def extract_metric_ids(value: Any) -> list[str]:
    ids = extract_numeric_ids(value)
    if ids:
        return ids
    fallback = str(value or "").strip()
    return [fallback] if fallback else []


def detect_id_keys(state_data: dict[str, Any], id_value: Any) -> set[str]:
    id_text = str(id_value or "").strip()
    keys = set()
    if not id_text or not isinstance(state_data, dict):
        return keys

    for ad in state_data.get("ad_state", []) or []:
        if isinstance(ad, dict) and str(ad.get("ad_id", "")).strip() == id_text:
            keys.add("ad_id")
            break

    for adv in state_data.get("adv_state", []) or []:
        if not isinstance(adv, dict):
            continue
        adv_id = adv.get("adv_id")
        if adv_id is None:
            meta = adv.get("meta_data", {}) or {}
            adv_id = meta.get("adv_id") if isinstance(meta, dict) else None
        if str(adv_id or "").strip() == id_text:
            keys.add("adv_id")
            break

    return keys


def detect_id_key(state_data: dict[str, Any], id_value: Any) -> str | None:
    keys = detect_id_keys(state_data, id_value)
    if "ad_id" in keys:
        return "ad_id"
    if "adv_id" in keys:
        return "adv_id"
    return None


class MetricHelpers:

    @staticmethod
    def extract_numeric_values_in_range(series_map, start_date, end_date):
        if not series_map:
            return []
        if isinstance(series_map, (list, tuple)):
            return [
                float(value)
                for value in series_map
                if isinstance(value, (int, float))
            ]
        values = []
        for key, value in series_map.items():
            try:
                d = datetime.date.fromisoformat(str(key))
            except Exception:
                continue
            if start_date and d < start_date:
                continue
            if end_date and d > end_date:
                continue
            if isinstance(value, (int, float)):
                values.append(float(value))
        return values

    @classmethod
    def sum_numeric_values_in_range(cls, series_map, start_date, end_date):
        return sum(
            cls.extract_numeric_values_in_range(series_map, start_date, end_date)
        )

    @staticmethod
    def safe_divide(numerator, denominator, default=0.0):
        try:
            num = float(numerator)
            den = float(denominator)
        except Exception:
            return default
        if den == 0:
            return default
        return num / den

    @classmethod
    def calc_ratio_from_series(
        cls, numerator_series, denominator_series, start_date, end_date
    ):
        if not numerator_series or not denominator_series:
            return 0.0
        if isinstance(numerator_series, (list, tuple)) and isinstance(
            denominator_series, (list, tuple)
        ):
            daily_ratios = []
            for n, dn in zip(numerator_series, denominator_series):
                if (
                    not isinstance(n, (int, float))
                    or not isinstance(dn, (int, float))
                    or dn == 0
                ):
                    continue
                daily_ratios.append(round(float(n) / float(dn), 6))
            if not daily_ratios:
                return 0.0
            return round(sum(daily_ratios) / len(daily_ratios), 6)
        daily_ratios = []
        for key in numerator_series:
            try:
                d = datetime.date.fromisoformat(str(key))
            except Exception:
                continue
            if start_date and d < start_date:
                continue
            if end_date and d > end_date:
                continue
            n = numerator_series.get(key)
            dn = denominator_series.get(key)
            if (
                not isinstance(n, (int, float))
                or not isinstance(dn, (int, float))
                or dn == 0
            ):
                continue
            daily_ratios.append(round(float(n) / float(dn), 6))
        if not daily_ratios:
            return 0.0
        return round(sum(daily_ratios) / len(daily_ratios), 6)

    @classmethod
    def calc_sequential_stats(cls, series_map, start_date, end_date):
        if isinstance(series_map, (list, tuple)):
            return cls._calc_sequential_stats_from_list(series_map, integer=False)
        ranges = cls._sequential_ranges(series_map, start_date, end_date)
        if ranges is None:
            return None, None, None
        current_start, current_end, prev_start, prev_end = ranges
        cur_avg = cls.average(
            cls.extract_numeric_values_in_range(series_map, current_start, current_end)
        )
        prev_avg = cls.average(
            cls.extract_numeric_values_in_range(series_map, prev_start, prev_end)
        )
        cur_avg = round(cur_avg, 4) if cur_avg is not None else None
        prev_avg = round(prev_avg, 4) if prev_avg is not None else None
        if cur_avg is None or prev_avg is None or prev_avg == 0:
            return cur_avg, prev_avg, 0.0
        return cur_avg, prev_avg, round((cur_avg - prev_avg) / prev_avg, 6)

    @classmethod
    def calc_sequential_stats_integer(cls, series_map, start_date, end_date):
        if isinstance(series_map, (list, tuple)):
            return cls._calc_sequential_stats_from_list(series_map, integer=True)
        ranges = cls._sequential_ranges(series_map, start_date, end_date)
        if ranges is None:
            return None, None, None
        current_start, current_end, prev_start, prev_end = ranges
        cur_avg = cls.average(
            cls.extract_numeric_values_in_range(series_map, current_start, current_end)
        )
        prev_avg = cls.average(
            cls.extract_numeric_values_in_range(series_map, prev_start, prev_end)
        )
        cur_avg = int(cur_avg) if cur_avg is not None else None
        prev_avg = int(prev_avg) if prev_avg is not None else None
        if cur_avg is None or prev_avg is None or prev_avg == 0:
            return cur_avg, prev_avg, 0.0
        return cur_avg, prev_avg, round((cur_avg - prev_avg) / prev_avg, 6)

    @classmethod
    def calc_sequential_stats_for_fraction(
        cls, numerator_series, denominator_series, start_date, end_date
    ):
        if isinstance(numerator_series, (list, tuple)) and isinstance(
            denominator_series, (list, tuple)
        ):
            return cls._calc_sequential_stats_for_fraction_from_lists(
                numerator_series,
                denominator_series,
            )
        ranges = cls._sequential_ranges_for_series_maps(
            [numerator_series or {}, denominator_series or {}],
            start_date,
            end_date,
        )
        if ranges is None:
            return None, None, None
        current_start, current_end, prev_start, prev_end = ranges
        cur_rate = cls.calc_ratio_from_series(
            numerator_series, denominator_series, current_start, current_end
        )
        prev_rate = cls.calc_ratio_from_series(
            numerator_series, denominator_series, prev_start, prev_end
        )
        if prev_rate == 0:
            return cur_rate, prev_rate, 0.0
        return cur_rate, prev_rate, round((cur_rate - prev_rate) / prev_rate, 6)

    @staticmethod
    def calc_bench_compare(current_value, bench_value):
        cur = float(current_value) if isinstance(current_value, (int, float)) else 0.0
        bench = float(bench_value) if isinstance(bench_value, (int, float)) else 0.0
        if bench == 0:
            return "高于同行", 0.0
        diff_pct = (cur - bench) / bench * 100.0
        return ("高于同行" if diff_pct >= 0 else "低于同行"), round(abs(diff_pct), 4)

    @staticmethod
    def fmt4(value):
        return f"{value:.4f}".rstrip("0").rstrip(".")

    @staticmethod
    def average(values):
        if not values:
            return None
        return sum(values) / float(len(values))

    @classmethod
    def calc_sequential_ratio(cls, series_map, start_date, end_date):
        if isinstance(series_map, (list, tuple)):
            cur_avg, prev_avg, ratio = cls._calc_sequential_stats_from_list(
                series_map,
                integer=False,
            )
            if cur_avg is None or prev_avg is None:
                return 0.0
            return [prev_avg, cur_avg, ratio]
        ranges = cls._sequential_ranges(series_map, start_date, end_date)
        if ranges is None:
            return None
        current_start, current_end, prev_start, prev_end = ranges
        cur_avg = cls.average(
            cls.extract_numeric_values_in_range(series_map, current_start, current_end)
        )
        prev_avg = cls.average(
            cls.extract_numeric_values_in_range(series_map, prev_start, prev_end)
        )
        cur_avg = round(cur_avg, 4) if cur_avg is not None else None
        prev_avg = round(prev_avg, 4) if prev_avg is not None else None
        if cur_avg is None or prev_avg is None or prev_avg == 0:
            return 0.0
        return [prev_avg, cur_avg, round((cur_avg - prev_avg) / prev_avg, 6)]

    @staticmethod
    def parse_percent_to_ratio(value):
        if value is None:
            return None
        if isinstance(value, (int, float)):
            x = float(value)
            return x / 100.0 if x > 1.0 else x
        s = str(value).strip()
        if not s:
            return None
        if s.endswith("%"):
            try:
                return float(s[:-1].strip()) / 100.0
            except Exception:
                return None
        try:
            x = float(s)
        except Exception:
            return None
        return x / 100.0 if x > 1.0 else x

    @staticmethod
    def resolve_date_range_from_series(series_maps, start_date, end_date):
        if start_date and end_date:
            return start_date, end_date
        if start_date and not end_date:
            return start_date, start_date
        if end_date and not start_date:
            return end_date, end_date
        date_keys = []
        for series_map in series_maps or []:
            if isinstance(series_map, (list, tuple)):
                continue
            for key in (series_map or {}).keys():
                try:
                    date_keys.append(datetime.date.fromisoformat(str(key)))
                except Exception:
                    continue
        if not date_keys:
            return None, None
        d = max(date_keys)
        return d, d

    @staticmethod
    def parse_duration_seconds(value):
        if value is None:
            return None
        if isinstance(value, (int, float)):
            return float(value)
        s = str(value).strip()
        if not s:
            return None
        if s.endswith("秒"):
            s = s[:-1].strip()
        try:
            return float(s)
        except Exception:
            return None

    @classmethod
    def _sequential_ranges(cls, series_map, start_date, end_date):
        return cls._sequential_ranges_for_series_maps(
            [series_map or {}], start_date, end_date
        )

    @staticmethod
    def _sequential_ranges_for_series_maps(series_maps, start_date, end_date):
        date_keys = []
        for series_map in series_maps or []:
            if isinstance(series_map, (list, tuple)):
                continue
            for key in (series_map or {}).keys():
                try:
                    date_keys.append(datetime.date.fromisoformat(str(key)))
                except Exception:
                    continue
        date_keys = sorted(set(date_keys))
        if not date_keys:
            return None
        current_end = end_date or date_keys[-1]
        current_start = start_date or current_end
        days = (current_end - current_start).days + 1
        if days <= 0:
            return None
        prev_end = current_start - datetime.timedelta(days=1)
        prev_start = current_start - datetime.timedelta(days=days)
        return current_start, current_end, prev_start, prev_end

    @classmethod
    def _calc_sequential_stats_from_list(cls, series, integer=False):
        values = [float(value) for value in series if isinstance(value, (int, float))]
        if len(values) < 2:
            return None, None, None
        split_index = len(values) // 2
        prev_values = values[:split_index]
        cur_values = values[split_index:]
        cur_avg = cls.average(cur_values)
        prev_avg = cls.average(prev_values)
        if integer:
            cur_avg = int(cur_avg) if cur_avg is not None else None
            prev_avg = int(prev_avg) if prev_avg is not None else None
        else:
            cur_avg = round(cur_avg, 4) if cur_avg is not None else None
            prev_avg = round(prev_avg, 4) if prev_avg is not None else None
        if cur_avg is None or prev_avg is None or prev_avg == 0:
            return cur_avg, prev_avg, 0.0
        return cur_avg, prev_avg, round((cur_avg - prev_avg) / prev_avg, 6)

    @classmethod
    def _calc_sequential_stats_for_fraction_from_lists(
        cls,
        numerator_series,
        denominator_series,
    ):
        pairs = [
            (float(n), float(dn))
            for n, dn in zip(numerator_series, denominator_series)
            if isinstance(n, (int, float))
            and isinstance(dn, (int, float))
            and dn != 0
        ]
        if len(pairs) < 2:
            return None, None, None
        split_index = len(pairs) // 2

        def ratio_for(items):
            if not items:
                return None
            ratios = [round(n / dn, 6) for n, dn in items]
            return round(sum(ratios) / len(ratios), 6)

        prev_rate = ratio_for(pairs[:split_index])
        cur_rate = ratio_for(pairs[split_index:])
        if cur_rate is None or prev_rate is None or prev_rate == 0:
            return cur_rate, prev_rate, 0.0
        return cur_rate, prev_rate, round((cur_rate - prev_rate) / prev_rate, 6)
