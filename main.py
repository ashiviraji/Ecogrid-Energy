
#----------user and meter services----------
import user_service
import meter_service



# ----- Pretty printer ----------------

def section(title: str):
    width = 60
    print(f"\n{'═' * width}")
    print(f"  {title}")
    print(f"{'═' * width}")


def subsection(title: str): 
    print(f"\n - {title} {'-' * (50 - len(title))}")


def print_dict_list(items: list):
    if not items:
        print("    (none)")
        return
    for item in items:
        for k, v in item.items():
            if v is not None:
                print(f"    {k:<25} {v}")
        print()


#--------main demo-------------------
def main():
    # -----User Management section--------
    #--------------------------
    section("STEP 1 — Register Participants (User Management Service)")
    #--------------------------

    print("\n  Registering sellers (solar panel owners)...")
    alice = user_service.register_user("Alice",  "alice@ecogrid.io",  "SELLER", initial_balance=0.0)
    bob   = user_service.register_user("Bob",    "bob@ecogrid.io",    "SELLER", initial_balance=0.0)

    print("\n  Registering buyers (energy consumers)...")
    carol = user_service.register_user("Carol",  "carol@ecogrid.io",  "BUYER",  initial_balance=50.0)
    dave  = user_service.register_user("Dave",   "dave@ecogrid.io",   "BUYER",  initial_balance=30.0)
    #--------------------------


    #--------meter readings ingestion-------
    #--------------------------
    section("STEP 3 — Ingest Smart Meter Readings (Smart Meter Service → Kafka [meter.readings])")
    #--------------------------

    print()
    meter_service.ingest_reading("METER-A1", kwh_available=5.2, kwh_consumed=1.8)
    meter_service.ingest_reading("METER-B2", kwh_available=3.0, kwh_consumed=2.5)

    #--------------------------

    #--------------------------
    section("STEP 10 — Final System State Summary")
    #--------------------------

    subsection("Users & Balances")
    print_dict_list(user_service.list_users())

    subsection("Meter Readings")
    print_dict_list(meter_service.get_all_readings())

    


if __name__ == "__main__":
    main()