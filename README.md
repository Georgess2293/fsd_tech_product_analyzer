
<img  src="title1.svg"/>

<div>

> Hello world! This is the project’s summary that describes the project, plain and simple, limited to the space available.
**[PROJECT PHILOSOPHY](#project-philosophy) • [PROTOTYPING](#prototyping) • [TECH STACKS](#stacks) • [IMPLEMENTATION](#demo) • [HOW TO RUN?](#run)**

</div> 
  

<br><br>

<!-- project philosophy -->

<a  name="philosophy" ></a>
<img  src="title2.svg" id="project-philosophy"/>

> Tech Product Analyzer is a Python-based ETL (Extract, Transform, Load) project designed to gather and analyze data from various web sources. The primary focus of this project is to provide comprehensive insights into tech products, with a particular emphasis on smartphones. This is achieved by extracting user reviews and preferences, transforming the data, and loading it into a PostgreSQL database for in-depth analysis. 

<br>

  

### User Types

 

1. Data Engineers.
2. Data Analysts.
3. Tech Influencers .
4. Smartphones shop owners.

  

<br>

  

### User Stories

  
1. As a Data Engineer:
	I want to automatically scrape various and new released products from reputable sources with the latest reviews so that our dataset is always up-to-date.
	I want to integrate different data sources seamlessly.
	Ensure fault tolerance in our data pipelines, so that potential failures don't interrupt our analyses.
2. As an Analyst:
	I want to query the database.
	I want to view the sentiment analysis results to understand users sentiment about different brands and products.
	I want to visualize the data using PowerBI.
3. As a Tech Influencer:
	I want to access detailed and up-to-date information about the latest tech products, with a particular focus on smartphones.
	I need the ability to track trends and user sentiments, enabling me to create relevant and engaging content for my audience.
4. As a Smartphone Shop Owner:
	I want insights into customer preferences and trending smartphone models to inform my inventory management and marketing strategies..
	I want to track product performance and customer reviews to ensure that the products I offer align with market demands.


<br><br>

<!-- Prototyping -->
<img  src="title3.svg"  id="prototyping"/>

> We have designed our projects to webscrape, through an ETL project and including it in a PowerBI Sample Dashboard, 

  

### Logger File

  

| Bins Map screen | Dashboard screen | Bin Management screen |

| ---| ---| ---|

| ![Landing](./readme/wireframes/web/map.png) | ![Admin Dashboard](./readme/wireframes/web/dashboard.png) | ![User Management](./readme/wireframes/web/bin_crud.png) |

  
  

### Data Flow Diagrams

  

| Map screen | Dashboard screen | Bin Management screen |

| ---| ---| ---|

| ![Map](readme/mockups/web/map.png)| ![Map](./readme/mockups/web/dashboard.png)| ![Map](./readme/mockups/web/bin_crud.png)|

  
  

| Announcements screen | Login screen | Landing screen |

| ---| ---| ---|

| ![Map](readme/mockups/web/announcements.png)| ![Map](./readme/mockups/web/login.png)| ![Map](./readme/mockups/web/landing.png)|

<br><br>

  

<!-- Tech stacks -->

<a  name="stacks"></a>
<img  src="title4.svg" id="stacks" />

<br>

  


  

## Frontend

Interactive PowerBI Dashboard:
A central dashboard where viewers can view:

1. Market Indicator: Graphs, charts and visualizations displaying yearly sales of manufacturers.
2. Sentiment Analysis: Representations of consumers sentiment about different products over time through pie charts and bars.
3. Specs analysis: Display correlation between the specs and the reviews.
4. Interactive filters: options to filter data by date, brand, or product for customized views.


  

<br>

  

## Backend

1. Web scraping & Automation.
2. ETL Pipeline: using python and pandas, raw data is extracted, transformed into a usable format and loaded into postgreSQL database.
3. Database: Schema Design - Indexing - Data Integrity.

<br>

<br>

  

<!-- Implementation -->

<a  name="Demo"  ></a>
<img  src="title5.svg" id="#demo"/>

> Show command line of ETL performance - Logger view

  
### App


| Dashboard Screen | Create Bin Screen |

| ---| ---|

| ![Landing](./readme/implementation/dashboard.gif) | ![fsdaf](./readme/implementation/create_bin.gif) |

  

| Bins to Map Screen |

| ---|

| ![fsdaf](./readme/implementation/map.gif) |

  
  

| Filter Bins Screen | Update Pickup Time Screen |

| ---| ---|

| ![Landing](./readme/implementation/filter_bins.gif) | ![fsdaf](./readme/implementation/update_pickup.gif) |

  
  

| Announcements Screen |

| ---|

| ![fsdaf](./readme/implementation/message.gif)|

  
  

| Change Map Screen | Edit Profile Screen |

| ---| ---|

| ![Landing](./readme/implementation/change_map.gif) | ![fsdaf](./readme/implementation/edit_profile.gif) |

  
  

| Landing Screen |

| ---|

| ![fsdaf](./readme/implementation/landing.gif)|

  

<br><br>




<!-- How to run -->

<a  name="run"  ></a>
<img  src="title6.svg" id="run"/>
  

> To set up ## **Tech Product Analyzer** follow these steps:

### Prerequisites


**Hardware & Software**:

-   A computer/server with sufficient RAM and processing power.
-   Operating system: Linux (preferred for production) or Windows.
-   Required software: Python (3.x), PostgreSQL, Git (for version control), and any other specific software packages.
  
  

**Dependencies**:

-   Install the necessary Python libraries by referring to the requirements.txt file:

```sh

pip install -r requirements.txt

```

-   Install database connectors/drivers for PostgreSQL.
  

### **Setting Up the Environment**:

**Clone the Repository**:


```sh

git clone https://github.com/Georgess2293/fsd_tech_product_analyzer.git

```

  
**Set Up the Database**:

-   Start the PostgreSQL server.
-   Create a new database and user with the appropriate permissions.
-   Run any initialization scripts to set up tables or initial.

### **Running the Backend**:

**Start the Data Ingestion & ETL Process**:
`python data_ingestion_script.py`


As for the dashboard access: Please use this link "public powerbi link" to access your data.
