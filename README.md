# Covid19-Analysis

In this project of the [Interaction Data Lab](https://research.cri-paris.org/teampage?id=5cde7f999a474e4a9f93b281) team at CRI, we aim at analyzing how tweets about self-reported symptoms can help us predict the evolution of the COVID19 pandemy. We first focused on the Paris region (Île-de-France).

Presentation of this repository:
1. Streaming data from Twitter API. We streamed data for several weeks using Twitter API, restricting the geolocalization to Île-de-France. This allowed us to get IDs from users geolocalized in Île-de-France.
2. History. To follow the same users, we collected the historical data (timelines) of the users we geolocated in Île-de-France.
3. Analyses. We studied the correlation between the number of tweets mentioning symptoms, and [official data from Sante Publique France](https://www.data.gouv.fr/en/datasets/donnees-des-urgences-hospitalieres-et-de-sos-medecins-relatives-a-lepidemie-de-covid-19/). 
The file Dashboard.ipynb presents the results and visualisations of the analyses.

The files Project_COVID_ENSAE.ipynb and Project_COVID_ENSAE.html are the ones I presented to validate a class at [ENSAE IP Paris](https://www.ensae.fr/) (Grade: 17/20).
