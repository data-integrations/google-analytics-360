# Google Analytics 360 batch source

Description
-----------
This plugin is used to query Google Analytics Reporting API.

Using this plugin you can get metrics and dimensions with built-in set. 

Also the API allows you to request combination of metrics expressed in mathematical operations. 
For example, you can use the expression ga:goal1completions/ga:sessions to request the goal completions per number of sessions

Please, see _https://developers.google.com/analytics/devguides/reporting/core/v4_ to get more information.

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

**Sampling Level:** Desired report sample size.

### Credentials

**Authorization token:** Authorization token to be used to authenticate to Google Analytics Reporting API.
