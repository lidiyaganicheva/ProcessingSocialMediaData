1. Upload dataset https://www.kaggle.com/datasets/smagnan/1-million-reddit-comments-from-40-subreddits/data
unzip file and place it in the folder "homework_2". 
File name should be kaggle_RC_2019-05.csv. If not - rename it in config.py
2. Run docker compose up -d
3. Run generator.py
4. To check message structure in broker container run:
./kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic reddit-comments --max-messages=5
5. Change number of partitions in config.py file
