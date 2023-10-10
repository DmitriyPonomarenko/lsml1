!!!WARNING!!!

The results of the algorithms differ because, in the source data, time is stored with accuracy to the second, which leads to the presence of more than one page with the same load time in a single session. When one session is split into multiple Spark partitions, the sorting result will not be uniquely determined after merging.




Description
Clickstream is a sequence of user actions on a website. It allows you to understand how users interact with the site. In this task, you need to find the most frequent custom routes.

Input data
Input data is а table with clickstream data in file hdfs:data/clickstream.csv.

Table structure
user_id (int) - Unique user identifier.
session_id (int) - Unique identifier for the user session. The user's session lasts until the identifier changes.
event_type (string) - Event type from the list:
page - visit to the page
event - any action on the page
<custom> - string with any other type
event_type (string) - Page on the site.
timestamp (int) - Unix-timestamp of action.

Browser errors
Errors can sometimes occur in the user's browser - after such an error appears, we can no longer trust the data of this session and all the following lines after the error or at the same time with it are considered corrupted and should not be counted in statistics.

When an error occurs on the page, a random string containing the word error will be written to the event_type field.

Sample of user session


Correct user routes for a given user:
Session 507: main-family-main
Session 514: main-archive-bonus-tariffs

Route elements are ordered by the time they appear in the clickstream, from earliest to latest.

The route must be accounted for completely before the end of the session or an error in the session.

Task
You need to use the Spark SQL, Spark RDD and Spark DF interfaces to create a solution file, the lines of which contain the 30 most frequent user routes on the site.

Each line of the file should contain the `route` and `count` values separated by tabs, where:
route - route on the site, consisting of pages separated by "-".
count - the number of user sessions in which this route was.

The lines must be ordered in descending order of the count field.
