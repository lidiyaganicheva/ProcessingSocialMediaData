1. Upload dataset https://github.com/linanqiu/reddit-dataset/blob/master/entertainment_comicbooks.csv
unzip file and place it in the folder "GroupProject". 
File name should be 'entertainment_comicbooks.csv', if not - rename it in config.py

2. Activate virtual environment and install required libraries from `requirements.txt`

3. Run 
```
    docker compose up -d
```

4. Run generator
```
    python generator.py
```

5. To check message structure in broker container run:
./kafka-console-consumer.sh --bootstrap-server localhost:29092 --topic reddit-comments --max-messages=5

6. To detect the language run language_detector.py

7. To detect the sentiment run sentiment.py

8. To extract keywords from comments, run keyword_extractor.py:
python keyword_extractor.py

9. To check extracted keywords, run in broker container:
./kafka-console-consumer.sh --bootstrap-server localhost:29092 --topic reddit-keywords --max-messages=5

The keyword extractor service:
- Reads messages from 'reddit-comments' topic
- Extracts the most common words (keywords) from each comment
- Sends results to 'reddit-keywords' topic
- Results include: comment ID, extracted keywords, and timestamp
