# Bufferqueue

Requirements
- Python 2.7

Features
- In memory pub sub
- Multiple publishers and subscribers supported
- Supported data types- int,string,list,dict
- Buffer size is 1MB (configurable in constants.py file)

Steps to run

  - Clone the project
  - Run 'Python run_queue_server.py'
  - Run 'Python subscriber_example.py'
  - Run 'Python producer_example.py'

When you run producer_example.py, you will be able to see data flowing from publisher to subscriber in console

You can modify the buffer size while creating queue in producer_example.py

Future Enhancements
- Sending and receving data in chunks
- Distributed architecture for queues
- Separate produce, consumer and queue server code