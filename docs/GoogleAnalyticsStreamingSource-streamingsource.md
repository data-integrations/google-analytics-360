# Google Analytics 360 streaming source

Description
-----------
This plugin is used to query Google Analytics Reporting API.

Using this plugin you can get metrics and dimensions with built-in set. 

Please, see _https://developers.google.com/analytics/devguides/reporting/realtime/v3_ to get more information.

**Caution**: The Real Time Reporting API, in **limited beta**, is available for developer preview only.
You can find all additional information about this at the link above.

Properties
----------
### Basic

**Reference Name:** Name used to uniquely identify this source for lineage, annotating metadata, etc.

**Google Analytics View:** The Google Analytics view ID from which the data is retrieved.

**Start Date:** Start date for the report data.

**End Date:** End date for the report data.

**Metrics:** Quantitative measurements. For example, the metric ga:users indicates the total number of users for the requested time period.

**Dimensions:** Attributes of your data. For example, the dimension ga:city indicates the city, for example, "Paris" or "New York".

### Advanced

**Poll interval:** The amount of time to wait between each poll in seconds.

### Credentials

**Authorization token:** Authorization token to be used to authenticate to Google Analytics Reporting API.
