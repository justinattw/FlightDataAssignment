# Flight Data Analysis

**Quantexa Coding Assignment - Justin Wong, 30 January 2021**

This is the repository for the Quantexa coding assignment, conducting exploratory data analysis into two csv files of 
flight data, containing data on flights and passengers. The scope and instructions of the assignment can be found in the
`Flight Data Coding Assignment - Instructions.docx` Word document of this directory.

This project is written in Scala and Spark, supported by sbt as the built tool and using IntelliJ IDEA.

## Table of Contents

- [Project Structure](#project-structure)
    - [Prerequisites, Platforms and Versions](#platforms-versions-and-prerequisites)
    - [Running the Project](#compiling-and-running-the-project)
- [About the Assignment](#about-the-assignment)
    - [Flight Data](#flight-data)
    - [Assignment Questions](#assignment-questions)
- [Solutions and implementations](#solutions-and-implementations)
    - [Assumptions of the data](#assumptions-of-the-data)
    - [Solution 1](#solution-1)
    - [Solution 2](#solution-2)
    - [Solution 3](#solution-3)
    - [Solution 4](#solution-4)
    - [Solution 5 (Bonus)](#solution-5-bonus)
    - [Checking my solutions](#checking-my-solutions)
- [Evaluation and Reflection](#evaluation-and-reflection)

---

## Project Structure

This Spark/ Scala project follows the project structure of a typical Scala repository, with the main Scala scripts
contained inside `.\src\main\scala`.

```
// The (noteworthy) files and directories contained in this repository. 

FlightDataAssignment
│   .gitignore
│   build.sbt
│   README.md
│   Flight Data Coding Assignment - Instructions.docx
|
└───src
    ├───main
    │   ├───data
    │   │       flightData.csv
    │   │       passengers.csv
    │   │
    │   ├───outSolutions
    │   │   ├───solution1
    │   │   │       part-00000-xxx.csv    
    │   │   │
    │   │   ├───solution2
    │   │   │       part-00000-xxx.csv    
    │   │   │
    │   │   ├───solution3
    │   │   │       part-00000-xxx.csv    
    │   │   │
    │   │   ├───solution4
    │   │   │       part-00000-xxx.csv    
    │   │   │
    │   │   └───solution5Bonus
    │   │           part-00000-xxx.csv    
    │   │   
    │   │
    │   └───scala
    │       │   Main.scala
    │       │
    │       └───Solutions
    │               Question1.scala
    │               Question2.scala
    │               Question3.scala
    │               Question4.scala
    │               QuestionBonus.scala
    │
    └───test
        └───scala
            ├───Data
            │       FlightDataTest.scala
            │       PassengersDataTest.scala
            │
            └───Solutions
                    Question1Test.scala
                    Question2Test.scala
                    Question3Test.scala
                    Question4Test.scala
                    QuestionBonusTest.scala
```

Within this directory, `Main.scala` contains the main script for solving the Questions posed in the *Flight Data Coding
Assignment*. This is where the inputs of the questions are defined, and the implementations of the solutions are called
(according to defined inputs).

The `Solutions` package contains most of the implementation of my responses to the five questions posed in the
assignment, arranged accordingly into Scala classes by its Question # `e.g. Solutions\Question1.scala`.

The csv data files to be analysed are stored in `.\src\main\data\`

The output of the programme (the solutions) are outputted in as .csv files within the `.\src\main\outSolutions\solution{x}` 
directories. They have the naming format `part-00000-xxx.csv` (this is because many Spark functionalities are done in
partitions).

The skeleton of the testing framework is also created, to give a rough idea of how the types of tests that should be 
implemented. Some basics tests are implemented, using 
[Scala Test](https://www.jetbrains.com/help/idea/run-debug-and-test-scala.html#test_scala_app).

### Platforms, Versions, and Prerequisites

This project was written in *Scala v2.12.13* and *Apache Spark v3.0.0*, which has dependencies on installing 
*Hadoop v2.2.0* and *Winutils* on the system. The project was built with the *sbt v1.4.6* build tool, and was developed 
in *IntelliJ IDEA*.

### Compiling and running the project

If the above platforms, version, and prerequisites are satisfied, you can run the project by going to the project
root directory in the command line, and doing: 

```
$ sbt compile
```

If everything is working properly, you should just see:

```
[success] Total time: 15 s, completed 02-Feb-2021 23:33:04
```

You can then run the project with:

```
$ sbt run
```

Alternatively, in IntelliJ IDEA, you can just use the GUI and click to run the `Main` class, which will build and run
the project altogether (this is what I use).

If the project runs successfully, the solutions will be printed to the terminal, and the results will be saved in the
`src\main\outSolutions` directory.

Tests aren't implemented yet, but if they are, they can be run with:

```
$ sbt run
```

---

## About the Assignment

### Flight Data

The flight data to be analysed includes two csv files, as follows:

```
// flightData.csv

Field       | Description
------------+----------------
passengerId | Integer representing the id of a passenger
flightId    | Integer representing the id of a flight
from        | String representing the departure country
to          | String representing the destination country
date        | String representing the date of a flight
```

```
// passengers.csv

Field       | Description
------------+----------------
passengerId | Integer representing the id of a passenger
firstName   | String representing the first name of a passenger
lastName    | String representing the last name of a passenger
```

### Assignment Questions

1. Find the total number of flights for each month.
2. Find the names of the 100 most frequent flyers.
3. Find the greatest number of countries a passenger has been in without being in the UK.
4. Find the passengers who have been on more than 3 flights together.
5. (Bonus) Find the passengers who have been on more than N flights together within the range (from,to).

---

## Solutions and implementations

These are some remarks I have on the implementations of my solutions to the assignment questions (there may be some 
repetition in the Scaladocs of the implemented classes).

### Assumptions of the data

Some of my implementations (well, virtually the only, but most notably solution 3) requires assumptions on the data. 
They are:

1.  Data includes passengers who only flies a maximum of a once a day. This is necessary as our datetime data does not
    contain information on the time, thus there is no definitive precise way for us to preserve the order of the data
    on the datetime level, only at the date level.
    - This assumption has been verified through pivot tables in Excel.
2. Data includes passengers who only travel between countries by flight. This means there can be no
   absence in passengers' country-movement tracking data.
    - This assumption can be formalised as `flightPath[i][to] == flightPath[i+1][from]`
    - For example: a traveller who flies `UK -> CN`, must fly `CN -> US`, then `US -> FR`. They may not cross
      the border by foot from `CN to US`, such that the data is represented as `UK -> CN, US -> FR`.
    - This assumption has been half-verified only by an eye test, skimming through certain cases in the data and 
      checking if the assumption holds for a few passengers. The verification is by no means robust.
    
While these assumptions have been half-verified manually, implementing test cases should be done to ensure robustness
to the analysis. I have included a rough skeleton of these two test cases in 
`.\src\test\scala\Data\FlightDataTest.scala` to give an idea of what the tests would look like.

### Solution 1

Drop duplicate records of `flightId` from `flightDataDF`, then count the number of records the remaining DF, grouped by 
`month` (extracted via the `date` column).

### Solution 2

Using `flightDataDF`, count the number of records grouped by `passengerId` and ordered by `count`. Join in 
`passengersDF` to get passenger names.

### Solution 3

The implementation of solution 3 involved the idea of repurposing the `from` and `to` columns into a continuous sequence
of each passenger's flight history. 

I felt that the most direct way would be to create a Sequence of tuples `(("UK", "DE"), ("DE", "NL"), ("NL", "US"), 
...)`, as Spark DF has built-in functionalities to handle this with `collect_list()` on the two columns, grouped by
`passengerId`. This Sequence of tuples representation of flight paths also addresses a potential fault in the 
[assumptions to the data](#assumptions-of-the-data) made in assumption 2, where passengers must fly out of a country
that they flew into.

An alternate Sequence form I considered was `["UK", "DE", "NL", "US", ...]`; however, this was not favoured, as 
applying `collect_list()` on only a`from` or `to` column would not capture the first fromCountry, or the last toCountry.
     
#### Tail-recursive algorithm in finding the longest run

Finding the longest run in the sequence of flights can be done in tail-recursion by maintaining two accumulator values 
within the recursive function's argument, on the current sequential run (without being in `excludeCountry`), and the 
longest sequential run.

This can take advantage of scala's tail-call optimisation using the method annotation `@tailrec`. 

#### Traversal (iterative) algorithm in finding the longest run

Obtaining each passenger's flight history in a Sequence format, we can simply traverse the sequence to find the longest
run out of specified country.

The implementation is done with a 'for' loop.

#### Potential alternate solution using 'rank'

An alternate solution I pondered was to make use of dataframes' rank functionality (`dense_rank()` in Spark) - by 
ranking flights grouped by passengerId and ordered by date, this would effectively provide a column which tells us the 
number of flights the passenger has taken within the DF timeframe (i.e. rank 1 means passenger's first flight of the
year, rank 3 means third flight).

From here, select records where `[to] == UK`, giving us the passengers who have been to the UK. Iterating 
chronologically and subtracting subsequent pairs of ranks (per passenger) will give the number of countries a passenger
has been to without being in the UK. Take the max number of this calculation (grouped by passengerId) to get the 
solution.

For passengers who have not been to the UK within the DF timeframe, you can get these passengers by doing some `outer
join` of the `flightDataDF` on the above set of passengerIds (passengers who have been to the UK). Then, simply take 
the max of the rank (grouped by passengerId).

Problems: Aside from some problems with iterating over a DF chronologically (Spark uses partitions so output is not
deterministic), this implementation would also face the problem that is present if using `collect_list()` to collapse
a `from`/ `to` column into a List[String] `["UK", "DE", "NL", "US", ...]` - there would be some more complications 
just to capture the first `from`/ last `to` country, which complicates the code and makes it less readable.



### Solution 4

#### Implemented solution

The Spark DF implementation for this solution was remarkably rudimentary (though thinking through the problem was not). 
To get pairwise comparisons of all passengers, we can simply the `flightDataDF` on itself, through `flightId`. This 
doubles passengerId in a pairwise manner into `passenger1Id` and `passenger2Id`. Each paired passenger is then therefore
joined the same number of times that they share a flight with each other.

To ensure we don't have duplicate paired-passengers bidirectionally or self-pairs, we select the DF where 
`passenger1Id < passenger2Id`. 

Finally, we count the number of records grouped by `passenger1Id` and `passenger2Id`, getting the value of the number of 
times a pair of passengers has shared flights.

We can then select where `[Number of flights together] > 3` to find the pair passengers who have shared more than 3 
flights together.

#### Mathematical approach with a bipartite graph (not implemented)

My first thought to solving this problem was to construct a bipartite graph using the `passengerId` and `flightId` 
columns of the `flightData` data represented by an N*M reduced adjacency matrix (where N=number of unique passengers, 
M=number of unique flights). Populating graph would traverse once through the dataframe once, adding links between the 
passenger and flights:

```
// flightData Spark DF

passengerId | flightId
------------+----------------
     24     |   978
     68     |   978
     87     |   978
     98     |   1064
     13     |   1064
     46     |   1064
     24     |   1423
     46     |   1423
     68     |   1423

// Reduced Adjacency Matrix - an M*N matrix (where N=number of unique passengers, M=number of unique flights)
 
        978  1064 1423
13    [[ 0,   1,   0],
24     [ 1,   0,   1],
46     [ 0,   0,   1],
68     [ 1,   0,   1],
87     [ 1,   0,   0],
98     [ 0,   1,   0]]

rows i_n: passenger indices   
cols j_m: flight indices

Example: matrix[13, 1064] = 1 indicates a bidirectional link from passengerId 13 -> flightId 1064 (i.e. passenger 13 
took flight 1064)
```

Given the above reduced adjacency matrix F of shape `M*N`, multiply F with transpose(F) (of shape `N*M`) to get a new 
`N*N` matrix, giving:

```
Result = F * Transpose(F)

         13   24   46   68   87   98
13    [[ 1,   0,   0,   0,   0,   1],
24     [ 0,   2,   1,   2,   1,   0],
46     [ 0,   1,   1,   1,   0,   0],
68     [ 0,   2,   1,   2,   1,   0],
87     [ 0,   1,   0,   1,   1,   0],
98     [ 0,   1,   0,   1,   0,   1]]
```

In the graph context, this gives the number of 2-path links from one node to another (i.e. between two indexes i -> j), 
through the flightID as the intermediate node.

Hence, to find the number of flights shared between passenger 24 and 68, we index into `Result[24][68]` (actually the 
2nd row, 4th column) to find that they shared two flights together in total (in this case = 2). 

The diagonal finds the number of 2-path links between a passengerId and itself, hence it gives the total number of 
flights that a passenger has been on, so this could have also been an implementation to Question 2.

As we know the resultant matrix is always symmetrical, the matrix multiplication process be further optimised. 
Furthermore, there are efficient methods to compute matrix multiplications of sparse matrices, so while the 
implementation has time complexity O(N*M), it has potential to be optimised further.

### Solution 5 (Bonus)

Question 5 was a short extension to question 4, asking that on top of finding the number of shared flights of paired 
passengers within a certain range of dates, to also retrieve the date of the first and last flights that were shared
between the paired passengers.

Firstly, we can truncate the `flightDataDF` to only contain records within the dates that we are interested in (`from`
and `to`) by filtering records so that `date >= from` and `date <= to`. 

Next, we need to get information on pairwise passengers, and the dates of their flights. This can be done by extending 
the `getPairwisePassengers` method described in [solution 4](#solution-4) to include the date column, which is possible 
as the date column is associated to the `flightId`. So, we join `flightDataDF` with itself on `flightId` while keeping
`date` as a column.

Then, as solution 4 simply conducted a count groupBy `passenger1Id` and `passenger2Id`, solution 5 does the same, but
extends the groupBy to aggregate count, min, and max on the `date` column to find the 'number of flights together', the 
'from' date (date of first shared flight), and the 'to' date (date of last shared flight).

Finally, we can just employ solution 4's implementation of `getMoreThanNFlightsShared` to find passengers who shared
flights at least N times.

### Checking my solutions

For questions 1 and 2, I was able to check and confirm my solutions by using Excel pivot tables. For questions 3 to 5 
however, unfortunately I was not able to conduct any robust tests to validate my response and solution output - I 
resorted to checking a few cases by hand, and can only confirm that in my few test cases the solutions found the right 
response, but that this is not exhaustive.

---

## Evaluation and Reflection

Learning a whole new programming paradigm is certainly challenging, and I could feel myself falling into old habits at
times. I definitely see the benefits of functional programming, though it requires a rewiring of the thought process of
problem solving. I don't really have any further evaluation other than what I have covered, though I wonder if my
implementations were optimal.

Thank you to Quantexa for providing me with this opportunity to work on a new language and learn a new programming 
paradigm.
 