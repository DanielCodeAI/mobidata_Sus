"""
savings_report.py — CLI-Tool für jährliche Pendler-Ersparnisse.

Berechnet basierend auf einer Route und 220 Arbeitstagen/Jahr:
- Jährliche CO2-Ersparnis (P+R und B+R separat)
- Bäume-Äquivalent (1 Baum ≈ 22 kg CO2/Jahr, Quelle: European Environment Agency)
- Kraftstoffersparnis in Euro (Benzinpreis: 1.75 €/L, Quelle: ADAC Durchschnitt DE 2024)
- Zeitdifferenz pro Tag und pro Jahr
- Jahres-Pendler-Score

Aufruf:
    python savings_report.py --start "Lörrach" --end "Freiburg" --arrival 08:30
"""

import argparse
import json
import sys

import httpx

API_BASE = "http://localhost:8000"
WORK_DAYS_PER_YEAR = 220

# Benzinpreis: 1.75 €/L (Quelle: ADAC Tankstellenpreise Durchschnitt Deutschland, Stand 2024)
# Verbrauch: 7.4 L/100km (Quelle: Kraftfahrt-Bundesamt, durchschnittlicher Pkw-Verbrauch DE 2023)
FUEL_PRICE_EUR_PER_L = 1.75
FUEL_CONSUMPTION_L_PER_100KM = 7.4

# 1 Baum absorbiert ca. 22 kg CO2/Jahr
# Quelle: European Environment Agency (EEA), "Carbon sequestration by European forests"
CO2_PER_TREE_KG = 22


def fetch_route(start: str, end: str, arrival: str) -> dict:
    """Call the /route endpoint."""
    resp = httpx.post(f"{API_BASE}/route", json={
        "start_address": start,
        "end_address": end,
        "arrival_time": arrival,
    }, timeout=30)
    resp.raise_for_status()
    return resp.json()


def print_divider(char="─", width=60):
    print(char * width)


def print_report(data: dict, start: str, end: str):
    car = data["car_only"]

    # Per trip (one way)
    car_km = car["distance_km"]
    car_min = car["duration_min"]
    car_co2_g = car["co2_g"]
    car_cost = car["cost_eur"]

    # Per day (round trip)
    daily_km = car_km * 2
    daily_co2_g = car_co2_g * 2
    daily_cost = car_cost * 2

    print()
    print_divider("═")
    print(f"  JÄHRLICHER PENDLER-SPARREPORT")
    print(f"  {start} → {end}")
    print_divider("═")
    print()
    print(f"  Basisdaten (eine Strecke):")
    print(f"    Auto-Distanz:   {car_km:.1f} km")
    print(f"    Auto-Dauer:     {car_min:.0f} Min")
    print(f"    Auto-CO2:       {car_co2_g} g")
    print(f"    Auto-Kosten:    {car_cost:.2f} €")
    print(f"    Arbeitstage/a:  {WORK_DAYS_PER_YEAR}")
    print()

    print_divider()
    print(f"  {'METRIK':<35} {'NUR AUTO':>10}")
    print_divider()

    yearly_co2_kg = daily_co2_g * WORK_DAYS_PER_YEAR / 1000
    yearly_cost = daily_cost * WORK_DAYS_PER_YEAR
    yearly_time_h = car_min * 2 * WORK_DAYS_PER_YEAR / 60

    print(f"  {'CO2/Jahr (kg)':<35} {yearly_co2_kg:>10.1f}")
    print(f"  {'Spritkosten/Jahr (€)':<35} {yearly_cost:>10.2f}")
    print(f"  {'Fahrzeit/Jahr (Stunden)':<35} {yearly_time_h:>10.1f}")
    print()

    for option_key, label in [("park_and_ride", "PARK & RIDE"), ("bike_and_ride", "BIKE & RIDE")]:
        opt = data.get(option_key)
        if not opt:
            print_divider()
            print(f"  {label}: Nicht verfügbar")
            print(f"    Grund: {data.get('reason', 'Unbekannt')}")
            print()
            continue

        opt_min = opt["total_duration_min"]
        opt_co2_g = opt["co2_g"]
        opt_cost = opt["cost_eur"]
        opt_score = opt["score"]

        # Daily (round trip)
        daily_opt_co2 = opt_co2_g * 2
        daily_opt_cost = opt_cost * 2
        daily_opt_min = opt_min * 2

        # Savings per day
        saved_co2_daily_g = daily_co2_g - daily_opt_co2
        saved_cost_daily = daily_cost - daily_opt_cost
        saved_time_daily = (car_min * 2) - daily_opt_min

        # Yearly
        saved_co2_yearly_kg = saved_co2_daily_g * WORK_DAYS_PER_YEAR / 1000
        saved_cost_yearly = saved_cost_daily * WORK_DAYS_PER_YEAR
        saved_time_yearly_h = saved_time_daily * WORK_DAYS_PER_YEAR / 60

        yearly_opt_co2_kg = daily_opt_co2 * WORK_DAYS_PER_YEAR / 1000
        yearly_opt_cost = daily_opt_cost * WORK_DAYS_PER_YEAR

        trees = saved_co2_yearly_kg / CO2_PER_TREE_KG if saved_co2_yearly_kg > 0 else 0

        print_divider("═")
        print(f"  {label}")
        print_divider("═")
        print()
        print(f"  {'METRIK':<35} {'WERT':>10} {'VS AUTO':>12}")
        print_divider()
        print(f"  {'Dauer/Fahrt (Min)':<35} {opt_min:>10.0f} {saved_time_daily/2:>+12.0f}")
        print(f"  {'CO2/Fahrt (g)':<35} {opt_co2_g:>10} {(car_co2_g - opt_co2_g):>+12}")
        print(f"  {'Kosten/Fahrt (€)':<35} {opt_cost:>10.2f} {(car_cost - opt_cost):>+12.2f}")
        print()
        print(f"  {'CO2/Jahr (kg)':<35} {yearly_opt_co2_kg:>10.1f} {saved_co2_yearly_kg:>+12.1f}")
        print(f"  {'Kosten/Jahr (€)':<35} {yearly_opt_cost:>10.2f} {saved_cost_yearly:>+12.2f}")
        print(f"  {'Fahrzeit/Jahr (Stunden)':<35} {daily_opt_min * WORK_DAYS_PER_YEAR / 60:>10.1f} {saved_time_yearly_h:>+12.1f}")
        print()
        print(f"  CO2-Ersparnis entspricht {trees:.1f} Bäumen/Jahr")
        print(f"    (1 Baum ≈ {CO2_PER_TREE_KG} kg CO2/Jahr, Quelle: EEA)")
        print()
        print(f"  Pendler-Score: {opt_score}/100")
        print()

    print_divider("═")
    print("  Quellen:")
    print("    CO2 Pkw: 152 g/km (UBA 2024, Emissionsdaten Personenverkehr)")
    print("    CO2 ÖPNV: 55 g/Pkm (UBA 2024, Nahverkehr Mix)")
    print(f"    Benzin: {FUEL_PRICE_EUR_PER_L} €/L (ADAC 2024)")
    print(f"    Verbrauch: {FUEL_CONSUMPTION_L_PER_100KM} L/100km (KBA 2023)")
    print("    Baum-Absorption: 22 kg CO2/Jahr (EEA)")
    print_divider("═")


def main():
    parser = argparse.ArgumentParser(description="Jährlicher Pendler-Sparreport")
    parser.add_argument("--start", required=True, help="Startadresse (Wohnort)")
    parser.add_argument("--end", required=True, help="Zieladresse (Arbeitsplatz)")
    parser.add_argument("--arrival", default="08:30", help="Ankunftszeit (HH:MM)")
    parser.add_argument("--json", action="store_true", help="Ausgabe als JSON")
    args = parser.parse_args()

    try:
        data = fetch_route(args.start, args.end, args.arrival)
    except httpx.ConnectError:
        print("FEHLER: Backend nicht erreichbar unter", API_BASE, file=sys.stderr)
        print("Starten Sie zuerst: uvicorn app:app --port 8000", file=sys.stderr)
        sys.exit(1)
    except httpx.HTTPStatusError as e:
        print(f"FEHLER: {e.response.status_code} — {e.response.text}", file=sys.stderr)
        sys.exit(1)

    if args.json:
        print(json.dumps(data, indent=2, ensure_ascii=False))
    else:
        print_report(data, args.start, args.end)


if __name__ == "__main__":
    main()
