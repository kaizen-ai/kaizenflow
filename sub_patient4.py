#Message filtering. Each caregiver can subscribe to a specefic patient to receive bi-daily updates on their medication and symptoms
import zmq

def create_subscriber():
    context = zmq.Context()
    socket = context.socket(zmq.SUB)
    socket.connect("tcp://localhost:5555")  
    socket.setsockopt_string(zmq.SUBSCRIBE, 'Patient_4_data') #filtering for patient_1_data

    while True:
        message = socket.recv_string()
        print(message)

create_subscriber()