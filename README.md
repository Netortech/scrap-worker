# Scrap Worker

## TL;DR

To run the project you must have Docker installed. From the repository root run the command:

  docker-compose up scrap-workers

For a quick example.

## A Little More Detail

This project represents an educational effort targeting the following concepts in a distributed context:

- [x] Automatic discovery and selection of leader.
- [x] Worker responsibilities.
  - [x] Provide heartbeat.
  - [x] Assigned work responsibilities.
    - [x] Consume assignments.
    - [x] Complete assignment.
    - [x] Notify leader of work completion.
- [ ] Leadership responsibilities.
  - [x] Detect any attempts to establish a new leader. 
  - [x] Detection of new workers.
  - [x] Detection of lost worker.
  - [x] Work distribution.
  - [ ] Work fault detection.
    - [ ] Worker died while attempting work.
    - [x] Worker did not complete work within alloted time.

To borrow a clever naming concept from Ben Eater, I would refer to this implementation as possibly "the worst distributed application ever"?
Where the comments, code or discussion purport to be the way things ought to be done are only meant in the context of the rest of the design
and not as how things should be done in an application which is to be used in a production setting.

# Expected Questions

Some questions may persist despite the disclaimer above.

## Why not use Kubernetes + [Serverless Technology]?

Great question, you should absolutely use Kubernetes plus your favorite serverless technology instead of this.

## Why not use Brooklin + Custom Connectors?

Also a great question, you should absolutely use Brooklin plus your custom connectors instead of this.

## Why didn't you use Zookeeper instead of rolling your own leader election?

Simply put, it fell within the scope of what I wanted to learn about in this effort. I've had no prior personal experience with these kinds of problems before and it's always interested me.

## Why did you use Kafka instead of using TCP or UDP socket connections?

Simply put, it fell out of scope of what I wanted to learn about in this effort. I've had some personal experience with socket connections in the past and didn't want to bother with the nitty gritty edge cases here.

## Some aspects of the project don't feel Pythonic. Why is that?

I'm primarily a .Net developer who has been working with Python for a couple years now. I've come to appreciate some common design themes in Python (longer method names feel better) but I continue to prefer classes over modules. Snake case still feels unnatural to me but I see the benefit to consistency over preference.

# General Design

Scrap workers are intended to be lightweight workers which can run in any context where Python 3.7.9 or later is available and the required pip packages can be installed. The usage of Docker in this repository is an attempt to remove many initial environment configuration hurdles. As mentioned in the TL;DR section above you can start up the application using

  docker-compose up scrap-workers

This will start up an instance of Kafka and Zookeeper which the five scrap worker replicas will use to intercommunicate. If you have your own Kafka cluster you wish to use you can change the configuration in the `./src/scrap_worker/config_manager.py` file. For more information about configuration management see the section below.

## Naming

The name Scrap Worker was chosen to convey the lightweight nature of the project. While it has not yet been actively tested these workers should be capable of running on something as small as a Raspberry pi. While everything is moving to the cloud, hardware based demonstrations continue to be a powerful way to educate engineers on the basics. Speaking from experience one of the most enjoyable experience I had during my college years was working with three different computers on a token ring network running at 300 baud to pass information arount. It is this experience I wish to share with other engineers but utilizing technologies they are likely to encounter in this day and age.

## Start Up Flow

Scrap Workers can be Leaders or Members of a Union. Every Scrap Worker starts out as a Member. When no Leader is communicating on the leadership topic a Member will switch to a Candidate and begin broadcasting it's candidacy on the election topic. Every Scrap Worker has a configurable Ranking and if it sees a Candidate with a higher ranking it will become a Voter and cast it's vote for that Candidate. Scrap Workers cannot be both Voters and Candidates. In the event there is a tie detected a Scrap Worker will randomly decrement it's ranking somewhere between 1-10. Eventually a single Scrap Worker will be the only Candidate remaining and it will begin an acceptance countdown. This acceptance countdown gives other Scrap Workers the opportunity to interject in the event that one of them still considers themselves a Candidate. The first Scrap Worker to broadcast a countdown value of 0 wins and becomes the Leader. The rest become Members and return to consuming the leadership topic where they expect the leader to communicate at least once within a configurable timeout.

# Configuration

Configuration can be found in the following file.

  ./src/scrap_worker/config_manager.py

## Custom Configurations

While you can make changes directly to the `Config` class it is recommended you instead use the `./src/scrap_worker/default_config.py` file. This file is ignored by git so changes to it cannot accidentally be checked in. If you don't see the `./src/scrap_worker/default_config.py` file it will be auto generated from `./src/scrap_worker/default_config.template` when you run the Docker `test` or `scrap-worker` services. Creating a file at runtime can occasionally introduce permissions complications. In these situations it's recommended to simply copy the template file manually.