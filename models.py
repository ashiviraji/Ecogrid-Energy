
# --------- begin smart meter models -----------------------

#import libraries
 
from dataclasses import dataclass   #to create data models
from datetime import datetime  #to store timestamp as datetime value

#smart meter reading class
@dataclass
class MeterReading:
    meter_id: str      #smart meter id
    household_id: str     #household id 
    timestamp: datetime      #reading was taken time
    generated_kwh: float   #generated energy amount from solar panel in kWh
    consumed_kwh: float   #used enery amount by household in kWh

    #extra energy calculation function
    def excess_energy(self) -> float:  #output stores as a decimal value
        return self.generated_kwh - self.consumed_kwh

    #function to check whether the house has extra energy
    def has_excess_energy(self) -> bool:  #output stores as a boolean value
        return self.excess_energy() > 0
    

# --------- end smart meter models -------------------------