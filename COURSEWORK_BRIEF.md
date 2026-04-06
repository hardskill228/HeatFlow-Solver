# Coursework Writing Brief

Use this brief as source material for generating a full coursework report in Ukrainian.

## Output Requirements

Generate a complete coursework text in Ukrainian for approximately 16 pages of A4 text.

Formatting target:

- font: Times New Roman
- size: 14 pt
- academic style
- coherent technical narrative
- no bullet overload in the final document
- use full paragraphs, transitions, and explanations

## Topic

Web application for solving a heat conduction problem with support for user authorization, task queueing, progress tracking, history of computations, and multi-service Docker deployment.

## Working Title

Development of a web application for numerical simulation of heat conduction processes with load balancing and asynchronous task execution.

## Core Idea of the Project

The project is a web system called `HeatFlow Solver`. It allows a user to:

- register and log in
- create a computational task with parameters
- run a heat conduction simulation
- observe execution progress in real time
- view final results
- store task history
- work with a PostgreSQL database
- run the system locally or through Docker Compose

The project combines mathematical modeling, backend development, frontend development, database design, asynchronous execution, and deployment.

## Purpose of the Coursework

The purpose of the coursework is to design and implement a web-oriented software system for modeling heat conduction processes, with support for:

- interactive user interaction through a browser
- secure user authentication
- asynchronous processing of heavy computations
- real-time status updates
- persistent storage of users and task history
- containerized deployment

## Object and Subject

Object of research:

web technologies for developing distributed applied systems with computational functionality.

Subject of research:

methods, models, and software tools for implementing a web application for heat conduction simulation using FastAPI, PostgreSQL, WebSocket communication, and Docker infrastructure.

## Relevance

The topic is relevant because numerical modeling of physical processes is actively used in engineering and scientific practice. At the same time, web technologies make such systems more accessible, because the user can interact with the computational module through a browser without installing complex local software. The work is also relevant due to the need to combine computational intensity with a convenient interface, server-side processing, task queueing, and result persistence.

## Main Functional Capabilities

The application supports the following functions:

1. User registration and login
2. Storage of user profile data
3. Creation of a computational task with configurable parameters
4. Launch of heat conduction simulation
5. Real-time progress monitoring
6. Display of worker/server that processes the task
7. Cancellation and pause/resume controls for tasks
8. Task history and profile page
9. Leaderboard page
10. PostgreSQL persistence
11. Docker-based deployment
12. Multi-service architecture with NGINX and two API instances

## Technologies Used

Backend:

- Python
- FastAPI
- Uvicorn

Frontend:

- HTML
- CSS
- Vanilla JavaScript

Database:

- PostgreSQL

Deployment and infrastructure:

- Docker
- Docker Compose
- NGINX

Other concepts:

- WebSocket
- REST API
- asynchronous communication
- task queue
- load balancing

## Architectural Description

The system has a client-server architecture.

The browser acts as the client layer. It provides:

- login and registration forms
- task creation form
- progress monitoring interface
- profile and history pages

The backend is implemented with FastAPI. It exposes HTTP endpoints for authentication, profile operations, task creation, task status retrieval, and task history. It also provides WebSocket endpoints for real-time progress updates.

The database layer is implemented with PostgreSQL. It stores:

- users
- tokens
- task queue data
- task history
- additional user profile information

The deployment layer contains Docker Compose configuration with several services:

- PostgreSQL container
- API server 1
- API server 2
- NGINX reverse proxy and load balancer

NGINX distributes incoming requests between API instances. This demonstrates practical elements of distributed system architecture and load balancing.

## Computational Part

The application imitates or performs a numerical solution of a heat conduction problem. The logic is implemented as a heavy backend computation that progresses through several stages:

1. initialization
2. mesh preparation
3. stiffness matrix assembly
4. iterative solver stage
5. result generation

During the computation, the system produces:

- current progress percentage
- stage description
- final maximum temperature
- final average temperature
- execution time
- time series data for visualization and analysis

The work should explain that the computational module is integrated into a web application, which is one of the important practical outcomes of the coursework.

## Database Description

The database is used not only for user authentication but also for computation lifecycle support.

The report should mention that PostgreSQL stores:

- registered users
- authentication tokens
- queued tasks
- active tasks
- completed task history
- result payloads and metadata

This makes the system more reliable than a purely in-memory solution, because state is preserved and can be reused across multiple API instances.

## Why Asynchronous Processing Is Needed

Heat conduction simulation is computationally expensive. If the server processed such tasks synchronously in the request thread, the interface would freeze and the user would receive a poor experience. Therefore, tasks are separated from the immediate request cycle. The system can start a task, return control to the user, and then stream progress updates while the computation continues.

This demonstrates one of the central engineering ideas of the coursework: separation of user interaction from long-running computation.

## Real-Time Progress Monitoring

An important feature of the system is task monitoring via WebSocket. The report should explain that unlike ordinary polling, WebSocket allows the server to deliver progress updates in near real time. This improves interactivity and makes the system suitable for long-running calculations.

Progress monitoring shows:

- current status
- execution stage
- worker identifier
- percentage of completion
- final result

## Load Balancing and Multi-Server Operation

One of the practical strengths of the coursework is the presence of two API servers behind NGINX. This is useful to describe as an element of scalability and fault tolerance.

The report should explain that:

- requests come to NGINX
- NGINX forwards them to one of two API servers
- both servers work with the same PostgreSQL database
- this architecture improves distribution of requests
- the approach reflects real deployment practices for web systems

## Security and Authentication

The application supports account creation and login. Authentication data is processed on the server side, and users are identified by tokens. The report should state that even though the project is educational, it includes a practical access-control mechanism and user separation.

## Pages and User Interface

The system contains several user-facing pages:

- login and registration page
- main application page for launching tasks
- profile page with user data and task history
- leaderboard page

The interface is built to allow a user to move from authentication to computation and then to result analysis without leaving the web environment.

## Practical Value

The practical value of the coursework is that it demonstrates a complete cycle of web application development:

- problem statement
- architecture design
- backend implementation
- frontend implementation
- database integration
- asynchronous communication
- Docker deployment

This makes the project suitable not only as an educational coursework but also as a prototype for more advanced engineering systems.

## What the Report Should Contain

Generate a full coursework structure with the following logical parts:

1. Title-like introductory part
2. Introduction
3. Relevance of the topic
4. Purpose and objectives of the work
5. Object and subject of research
6. Review of technologies and methods
7. System architecture
8. Description of backend implementation
9. Description of frontend implementation
10. Database design and data storage
11. Description of computational module
12. Task queue and progress monitoring
13. Docker deployment and multi-service infrastructure
14. Testing and practical verification
15. Advantages and limitations of the developed system
16. Conclusions

## Writing Instructions For ChatGPT

Write in formal Ukrainian academic style.

Requirements:

- do not write too briefly
- explain each section in full paragraphs
- add transitions between sections
- include technical details, but keep them readable
- do not invent technologies that are not used in the project
- do not switch to Russian or English except for technology names
- keep terminology consistent throughout the text

## Optional Extra Instruction

If needed, also generate:

- abstract
- conclusions
- list of keywords
- presentation theses for defense
- short annotation for the coursework

