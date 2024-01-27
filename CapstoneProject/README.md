# Game Recommendations

## Motivation
I simply adore it when Spotify suggests a track that I wouldn't have discovered on my own, but ends up becoming a new favorite of mine. As a gamer, I would like this feeling when opening steam to buy a game. I really don't wanna think too much about which one to buy and put a lot of research on in.

## Objective
This project aims to provide game suggestions based on the games you have played (in steam) the most in terms of hours. The suggested categories include games with up to 10 hours, up to 20 hours, up to 50 hours, and more than 50 hours of gameplay.


## Requirements
To obtain the necessary information, it's important that the user's profile is set to public on the Steam platform and that the user has their own steam_id.

### Setting Steam Profile to Public
To set your Steam profile to public, follow these steps:

1. Open the Steam client.<br>
2. Go to the "Profile" tab.<br>
3. Click on "Edit Profile".<br>
4. In the privacy settings, set your profile to public.<br>

### Obtaining your steam_id
The steam_id is a unique identifier associated with your Steam account. To find it, follow these steps:

1. Log in to your Steam account. <br>
2. In your web browser, go to the Steam homepage and log in if necessary. <br>
3. In the top right corner, click on your username and select "Profile". <br>
4. The URL in the browser will look something like "https://steamcommunity.com/id/your_username/". If your username is not numeric, you need to convert this to a steam_id. <br>
5. To do this, you can use online URL to steam_id conversion tools, or use the Steam API directly.<br>

## Project Data

### Data
The data used in this project comes from a public dataset available on Kaggle, consisting of approximately 475 thousand rows in CSV format (mainly for review purposes, exclude games that have bad reviews in the suggestion).<br>

Additionally, queries were made to the IGDB API, which returns data in JSON format, which has approximately 265 thousand rows.
Some fields are in array format, so the data was organized into tables (csv format) for analysis and processing. 
One of the tables "similar_games.csv" has x thousand rows. 


### APIS

#### Steam API Overview
SteamAPI is an API (Application Programming Interface) provided by Steam, a digital distribution platform for video games. It offers developers access to various services and functionalities provided by Steam, including user authentication, game ownership verification, friend lists, and game statistics retrieval.<br> 

One important aspect to note is that SteamAPI imposes a rate limit of one request every 1.5 seconds, meaning developers must be mindful of this limitation when making requests to the API. Despite this restriction, SteamAPI remains a valuable resource for accessing user-specific data and integrating.

#### IGDB API Overview
The IGDB (Internet Game Database) API provides developers with access to extensive video game data, including titles, genres, platforms, release dates, and ratings. With its comprehensive coverage and advanced search functionality, the IGDB API is a valuable resource for gaming enthusiasts and developers.<br>

The configuration set up can be found at: https://api-docs.igdb.com/<br>
More information can be found at: https://www.igdb.com/


## How It Works
To use this project, follow the steps below:

1. Clone this repository to your local environment. <br>
2. Make sure you have Python installed on your machine. <br>
3. Open a terminal in the project folder. <br>
4. Run the main program program.py and pass your steam_id as a parameter.<br>
5. The program will query your information on Steam, cross-reference this data with the categories of hours played, and provide game suggestions based on this information. <br>
6. The information is an excel file containing a list of games, sorted by reccomendation strength, with name, price and link to buy  ;)

### Architecture diagram
![Image Alt text](/arquitecture_diagram.png)

## Learning Insights
Throughout this project, I deepened my understanding of working with APIs, specifically IGDB and SteamAPI, and processing data in JSON and CSV formats. I realized the complexity of consolidating game data from various websites into a single database, highlighting the challenges of real-world data integration.

During development, I identified crucial parameters for effective game recommendations, such as game prices, region, and the minimum age requirement. However, while the minimum age requirement is important, it has not yet been integrated.

I also learned that I do need to study more regarding infrastructure, because most of things here could be much faster using cloud infrastructure. Everything runs local, with no distribution computational, which is fine for a POC, but very far from a MVP or product.

## Limitations
1. I acknowledge that the reliance on third-party data introduces limitations. Basically this recommendation is not very good because it lacks information.<br>
2. Everything is local in csv/json and not ideally in a pipeline with scheduler process and using cloud, which means that the whole process doesn't have a lot of data quality yet. <br>
Basically, if the process broke one time, I have an if statement.
3. SteamAPI limit for requests per second. (A product will face problems, since many people are using)
4. At present, no consideration has been given to other platforms beyond the current scope. Therefore, if you seek recommendations exclusively tailored to the PS5 platform, such functionality is not available at this time.
5. All keys are still in code.

## Next Steps
### Data:
1.**Expand Data Collection:** Utilize SteamAPI for prolonged periods, considering its rate limit of 1.5 seconds per request, to gather comprehensive data on games played by users and their corresponding playtimes. This approach aims to generate insights from "real-case scenarios," where traditional rules may not apply straightforwardly. <br>

2.**Incorporate Friend Data:** Integrate functionality to analyze games owned by a user's friends on Steam, enhancing recommendation accuracy by considering social connections.

3.**Monitor New Releases:** Implement mechanisms to track new game releases and assess their popularity among players, leveraging trends to enhance recommendation precision.

4.**Include Game Promotions:** Incorporate data on game promotions and discounts to provide users with insights into cost-effective gaming opportunities and enhance the platform's utility for budget-conscious gamers.

### Infrastructure

**Table and Pipeline Development:** Establish robust tables and pipelines to support the seamless operation of the recommendation system within a website environment, focusing solely on essential components for efficient functionality.
Future Enhancements:<br>
**Enhance User Interaction:** Implement prompt engineering and utilize ChatGPT to enhance the user experience, creating a more conversational tone akin to seeking recommendations from a friend.<br>
**Twitch Integration:** Integrate Twitch functionality to provide users with direct links to live streams of games they may be interested in, enhancing engagement and exploration opportunities.<br>

## Results

When executing steam_reccomendation.py with your Steam ID as an argument, an Excel list is generated, pre-sorted by recommendation strength. I will definetly try some of my list, which is also available in the files.

## Special Thanks

Thanks **Zach** and **TA's team** really appreciate being part of the bootcamp. The content really helped me facing problems from a different perspective and  has contributed significantly to my growth as a data engineer.

Final special thanks for my family and my father, that passed away in december 17, 2023.<br> 
I promissed him, I am committed to becoming the best Data Engineer that I can be and thats my felling: 
![Terminal](https://quotefancy.com/media/wallpaper/800x450/4675000-Kobe-Bryant-Quote-Great-things-come-from-hard-work-and.jpg)