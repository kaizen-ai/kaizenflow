#Message filtering. Each caregiver can subscribe to a specefic patient to receive bi-daily updates on their medication and symptoms
#In this example we are subscribing to two patients which could be useful if there is a psychiatrist responsible for multiple patients
import zmq

def create_subscriber():
    context = zmq.Context()
    socket = context.socket(zmq.SUB)
    socket.connect("tcp://localhost:5555")  
    socket.setsockopt_string(zmq.SUBSCRIBE, 'Patient_8_data') #filtering for patient_8_data
    socket.setsockopt_string(zmq.SUBSCRIBE, 'Patient_9_data') #filtering for patient_9_data

    while True:
        message = socket.recv_string()
        print(message)

create_subscriber()