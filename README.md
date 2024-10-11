# My-Uber: Distributed Taxi Simulation System

## Project Overview

"My-Uber" is a distributed system designed to simulate a taxi service similar to Uber. The project aims to implement concepts of distributed systems, such as asynchronous communication, fault tolerance, and data persistence. The system simulates taxis moving through a city represented by an `N x M` grid, where users request taxi services. A central server registers the positions of taxis within the city, receives service requests from users, and assigns the closest available taxi to the user. This simulation can help a small company evaluate whether it is feasible to enter the market with a small number of taxis.

## Main Features

- **City Simulation**: The city is represented as a grid of up to 1000x1000 cells, with taxis and users occupying various positions.
- **Taxi Movement**: Taxis move horizontally or vertically within the grid based on a specified speed (1, 2, or 4 km/h). When a taxi reaches the grid boundary without a service, it remains stationary.
- **Service Assignment**: The central server calculates the distance between taxis and users to assign the nearest taxi available.
- **Distributed Architecture**: The project uses [ZeroMQ](https://zeromq.org/languages/python/) for implementing communication patterns such as publish/subscribe and request/reply.
- **Fault Tolerance**: The system includes a backup server and a health-check process to monitor the central server's availability and switch to the backup server if necessary.

## Installation

### Prerequisites

- **Python 3.12.6 or higher**: Make sure you have at least Python 3.12.6 installed. You can download it from [python.org](https://www.python.org/downloads/).

### Setting Up the Project

1. **Clone the repository**:

```bash
git clone https://github.com/yourusername/my-uber.git
cd my-uber
```

2. **Create a Virtual Environment (Optional but Recommended)**:

```bash
python -m venv venv
source venv/bin/activate    # On Unix or MacOS
venv\Scripts\activate       # On Windows
```

3. **Install Dependencies**: Make sure to install all the required packages listed in the `requirements.txt` file.

```bash
pip install -r requirements.txt
```

## Running the Project

The system consists of three main components:

- **Dispatcher (Central Server)**
- **Taxis**
- **Users**

Each component is run as a separate process and can be executed on different machines or virtual machines.

**Step 1: Running the Dispatcher**

Start the dispatcher on one machine:

```bash
python src/dispatcher.py <N> <M>
```

Where `<N>` and `<M>` are the dimensions of the city grid (e.g., 10 10 for a 10x10 grid).

**Step 2: Running Taxis**

Run the taxis on one or more machines:

```bash
python src/taxi.py <taxi_id> <N> <M> <pos_x> <pos_y> <speed>
```

- `<taxi_id>`: Unique identifier for the taxi.
- `<N>` and `<M>`: Dimensions of the grid (same as used in the dispatcher).
- `<pos_x>` and `<pos_y>`: Initial position of the taxi in the grid.
- `<speed>`: Speed of the taxi (1, 2, or 4 km/h).


**Step 3: Running the User Generator**

Run the user generator to create service requests:

```bash
python src/user.py <Y> <N> <M> <coordinates_file>
```

- `<Y>`: Number of users.
- `<N>` and `<M>`: Dimensions of the grid.
- `<coordinates_file>`: Text file containing the initial coordinates of the users.

## Configuration

Configuration settings such as IP addresses, ports, and logging levels can be adjusted in `src/config.py`.

## Testing

Unit tests for the components are located in the `tests/` directory. You can run the tests using:

```bash
pytest tests/
```