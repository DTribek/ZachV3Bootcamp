# Infrastructure Track Apache Flink Training Homework

In this week, we'll assume you already have the compute environment set up from the week's content.

The submission for this week will consist of the following:

- Using Python, create a Flink job that sessionizes the input data by IP address and host, using a 5 minute gap.
- Write SQL to do the following:
  - Define a table to store the average number of web events in a session by hostname (`average_web_events_ddl`)
  - Insert the data into that table (`insert_average_web_events`)
  - Compare the average values between zachwilson.techcreator.io, zachwilson.tech, and lulu.techcreator.io (`select_average_web_events`)
