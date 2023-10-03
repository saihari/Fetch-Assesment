# Fetch-Assesment: ETL off a SQS Queue

## Update 3rd October, 2023

Resolved following issues:
    1. Implemented mechanisms to ensure duplicate messages are not pushed.
    2. Improved functionality to detect invalid messages.

## How to Run

1. Open a terminal and Clone the git repository. Enter the directory.

```bash
    git clone https://github.com/saihari/Fetch-Assesment.git
    cd Fetch-Assesment
```

2. Build the docker image using following command. Here I using the name of the docker image to be ```fetch-assignment```

```bash
    docker build --no-cache -t fetch-assesment.
```

3. Run the pre-defined docker images that contains the test data and target tables

```bash
    docker-compose up
```

4. Once both the process are up and running, open a new terminal in the same directory

```bash
    docker run --network="host"  fetch-assesment
```

## Approach

Retrieve messages from the sqs queue in batches, once each message is successfully retreived the message is deleted and then process set of messages i.e, masking them, addting the created_at column and rectifying any errors (if any) and push them to db. We concurrently process data as it is recieved to achieve near parallel processing, maximizing resoure utilization and speed.

## Design Thoughts

1. How will you read messages from the queue?

    We will read the messages in batches using the boto3 API. The batch size is determined by the minimum number of messages to be read divided by number of cpus available to process the data.

2. What type of data structures should be used?

    Primarily used the dictionary data structure available in python to retrieve sub fields like ip and device_type instantaneously. We also convert the dictionary into a pandas dataframe to avoid iterating through each message and individually pushing them into the database.

3. How will you mask the PII data so that duplicate values can be identified?

    We mask the PII data by hashing it using the SHA3 algorithm to produce a 512-bit (64-byte) hash value. Since, for the same string the hashing function produces same output we can identify duplicate values.

4. What will be your strategy for connecting and writing to Postgres?

    Each individual sub-process creates a connection  using the SQLAlchemy library of python to connect and write each batch of data to Postgres, i.e., each process with execute only 1 insert statement.

5. Where and how will your application run?

    I have currently packaged the application in an docker image and can be run using a docker run command on any compute environment that has docker installed and has internet connectivity.

## Questions

1. How would you deploy this application in production?

    If we productionize it in AWS we can run the docker image as a ECS task using fargate to keep it serverless. The ECS task would be configured such that the task would run periodically.  

2. What other components would you want to add to make this production ready?

    I would create another queue or s3 bucket to store and debug deformeded messages or failed messages. I would also place monitoring and alerting mechanisms to track resource utilization and send alerts if we hit/ are near resource limits. Add an application scerets manager to store database credentials

3. How can this application scale with a growing dataset?

    Upto a certain extent we can horizontally scale the system to process the message. If the dataset size increases exponentially we can have multiple sqs queues following a distributed logic (for example, like distributed by locale value or geographic region value) instead of having single a single queue and then we would have a one on one mapping of the application and queue.

4. How can PII be recovered later on?

    In this approach PII cannot be  recoved later-on by dcrypting the hash, But to recover PII later on we use other techniques like encrypting based on a key or tokenization or advanced data anonymization techniques.

5. What are the assumptions you made?

    I made 3 assumptions:

    1. I assumed that the PII information in this particular database would never be required to be recovered.
    2. The ```create_date``` column in the Postgres denotes the date when the message was processed but not created.
    3. The app version converted to integer by truncating the full string to the first character of the app version.

## Next Steps

1. I would create a cache to store the message ID of message received to ensure each messsage is processed only once. The cache could be cleared after a set time.

2. Explore advanced data anonymization techniques.

3. Optimize the batch size calculation.

4. Convert the static variables declared to application secrets and environment variables except the ```ALL_COLUMNS``` static variable.
