# Instructions
**1. Upload dataset https://github.com/linanqiu/reddit-dataset/blob/master/entertainment_comicbooks.csv**
unzip file and place it in the folder "GroupProject". 
File name should be 'entertainment_comicbooks.csv', if not - rename it in config.py

**2. Activate virtual environment and install required libraries from `requirements.txt`**
- Mac (zsh):
```zsh
    python3 -m venv venv
    source venv/bin/activate
    pip install -r requirements.txt
```
- Windows (cmd):
```cmd
    python -m venv venv
    .\venv\Scripts\activate.bat
    pip install -r requirements.txt
```

**3. Run **
```
    docker compose up -d
```

**4. Run generator (generates dataset)**
```
    python generator.py
```

**5. Run language_detector.py (detects language of a message)**
```
    python language_detector.py
```

**6. Run sentiment.py (detect sentiment)**
```
    python sentiment.py
```

**7. Run keyword_extractor.py (extracts keywords):**
```
    python keyword_extractor.py
```
The keyword extractor service:
- Reads messages from 'reddit-comments' topic
- Extracts the most common words (keywords) from each comment
- Sends results to 'reddit-keywords' topic
- Results include: comment ID, extracted keywords, and timestamp


**8. Run report.py (generates report in the report.json file)**
```
    python report.py
```




## Notes

To check message structure in broker container run:
```
./kafka-console-consumer.sh --bootstrap-server localhost:29092 --topic <topic_name> --max-messages=5
```

