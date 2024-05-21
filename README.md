## Dataset

This dataset, OneDayOfTweets.txt, was sourced from the link below. It contains a total of 4.4M tweets in text format. 
- [Tweets Data Source: OneDayOfTweets.txt](https://dbgroup.cdm.depaul.edu/DSC450/OneDayOfTweets.txt)


## Purpose

This project aims to create databases and retrieve information in various methods using this Twitter data and compare their performances.


## Part 1: Creating Databases using SQL

Creating databases with 110,000 tweets and 550,000 tweets and comparing runtime to create.

**1A: Write Data from Web to TXT File using Functions**
- This TXT file will be used for parts 1C, and 1D.

| Metric                                       | 110,000 Tweets | 550,000 Tweets |
|----------------------------------------------|----------------|----------------|
| Runtime (seconds) saving tweets in txt file | 14.56          | 69.67          |

**1B: Insert Data from Web using SQL**

| Metric           | 110,000 Tweets | 550,000 Tweets |
|------------------|----------------|----------------|
| Runtime (seconds) | 35.03          | 232.08         |

**1C: Insert Data from TXT File using SQL**

| Metric           | 110,000 Tweets | 550,000 Tweets |
|------------------|----------------|----------------|
| Runtime (seconds) | 4.77           | 24.49          |

**1D: Insert Batches of Data from TXT File using SQL**

| Metric           | 110,000 Tweets | 550,000 Tweets |
|------------------|----------------|----------------|
| Runtime (seconds) | 3.46           | 18.91          |


<img src="assets/img/1E_plot_runtime_populate.png" alt="plot_runtime_populate">


## Part 2: Find the average longitude and latitude value for each user ID 5 times vs. 20 times
- Compare runtime performance of SQL vs. Functions

2A:



<img src="assets/img/2G_plot_runtime_distributions.png" alt="plot_runtime_distributions.png">


  In summary

