1. Upload dataset https://github.com/linanqiu/reddit-dataset/blob/master/entertainment_comicbooks.csv
unzip file and place it in the folder "GroupProject". 
File name should be 'entertainment_comicbooks.csv', if not - rename it in config.py
2. Run docker compose up -d
3. Run generator.py
4. To check message structure in broker container run:
./kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic reddit-comments --max-messages=5


