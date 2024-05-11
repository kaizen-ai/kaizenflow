
import zmq
import time
import random
from datetime import datetime, timedelta

# First I will generate synthetic patient data for 25 patients
patients = [f'Patient_{i}' for i in range(1, 26)]  # Create patient IDs from 1 to 25

# Creating a list of the possible medications, symptoms, and dosages that a patient may have
medication = ['Prozac', 'Klonopin', 'Clozapine', 'Valproate', 'Lithium', 'Paxil']
dosage = ['5mg', '10mg', '20mg', '25mg', '30 mg', '50 mg']
symptoms = ['Fatigue', 'Loss of appetite', 'Insomnia', 'Suicidal thoughts', 'Increased appetite', 'No symptoms']
mood = ['happiness', 'anxiety', 'depression', 'euthymia', 'anger', 'sadness']

context_pub = zmq.Context()  # Create a ZeroMQ context for publisher
socket_pub = context_pub.socket(zmq.PUB)  # Create a publisher socket
socket_pub.bind("tcp://*:5555")  # Bind the socket to a network address

date = datetime.now().date()  # grabbing todays current date and we will simulate data starting from today

while True:  # Continuously send patient data
    for patient in patients:
        reported_symptoms = random.choice(symptoms)
        reported_mood = random.choice(mood)
        patient_medication = random.choice(medication)
        patient_dosage = random.choice(dosage)
        timestamp_noon = date.strftime('%Y-%m-%d 12:00:00')
        timestamp_midnight = date.strftime('%Y-%m-%d 00:00:00')
        try:
            data_noon = f"{date.strftime('%Y-%m-%d 12:00:00')}: your patient {patient} received {patient_dosage} of {patient_medication}. The patient reported having {reported_symptoms} and feelings of {reported_mood}. If you have any questions or concerns please call our facility at 240-123-4567 or email umdpsychiatrics@gmail.com!"
            data_midnight = f"{date.strftime('%Y-%m-%d 00:00:00')}: your patient {patient}  received {patient_dosage} of {patient_medication}. The patient reported having {reported_symptoms} and feelings of {reported_mood}. If you have any questions or concerns please call our facility at 240-123-4567 or email umdpsychiatrics@gmail.com!"
            topic = f"{patient}_data" #when filtering I needed to be more specefic because otherwise only specifying patient_1 also pulled patients 10-19
            socket_pub.send_string(f"{topic}: Dear Caregiver, on {timestamp_noon}{data_noon}")  # Send data for noon
            socket_pub.send_string(f"{topic}: Dear Caregiver, on {timestamp_midnight}{data_midnight}")  # Send data for midnight
          
        except Exception as e:
            print(f"An error occurred and no message has been sent: {e}")

    # Simulate a delay between updates
    time.sleep(60)
    # Move to the next day
    date += timedelta(days=1)
