
#------------begin smart meter execution------------

from meter_service import MeterService


def main():
    print("Starting EcoGrid Smart Meter Processing...\n")

    meter_service = MeterService()
    processed_results = meter_service.process_meter_readings()

    print("\nProcessed Results:")
    print(processed_results)


if __name__ == "__main__":
    main()

#------------end smart meter execution------------