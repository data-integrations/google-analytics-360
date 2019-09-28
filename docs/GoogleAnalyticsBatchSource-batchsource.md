# Google Analytics 360 batch source

Description
-----------
This plugin used to query Google Analytics Reporting API.

Properties
----------
### General

**Reference Name:** Name used to uniquely identify this source for lineage, annotating metadata, etc.

**Authorization token:** Authorization token to be used to authenticate in Google Analytics Reporting API.

**Google Analytics View:** The Google Analytics view ID from which to retrieve data.

### Basic

**Start Date:** Start date for the report data.

**End Date:** End date for the report data.

**Metrics:** Quantitative measurements. For example, the metric ga:users indicates the total number of users for the requested time period.

**Dimensions:** Attributes of your data. For example, the dimension ga:city indicates the city, for example, "Paris" or "New York".

### Advanced

**Sampling Level:** Desired report sample size.
