# Information_Retrieval_2019-2020_Project
This repository is supposed to host the code for a web crawler project, developed for the course of Information Retrieval held at UniTS during 2019/2020 academic year

## Running the Program
If you wish to run the program, just place yourself in the project directory and type on the command line:

`python3 main.py`

The following dependencies are guaranteed to be enough for the program to run:
* python==3.7
* bs4==4.8.1
* reppy=0.4.14
* requests==2.22.0
* urllib3==1.25.7

## Navigating the Project
The project consists of the following source files:
* `web_crawler.py` contains the `WebCrawler` class, supposed to manage and coordinate a team of "child" crawlers.
* `single_crawler.py` contains the "child" crawlers' class that extend the `threading.Thread` class and physically crawl the Web. 
* `frontier.py` contains the frontier implementation;
* `main.py` is a simple script to instantiate and launch a web crawler. The user is supposed to work on this file; in particular, he/she may decide what to do with the retrieved webpages. 

**If you wish to run the web crawler, be sure to edit the variable `my_user_agent` inside the `main.py` file to be whatever corresponds to your web browser.** 
