# FirstC--RestApiCalls
C# with rest api calls - first ever c# project 

Longtime RPG programmer on ibmi.  We needed to make rest api calls and we were not allowed out through the firewall.  
Solution - totally new.
A co-worker did proof of concept in C# and I did it in PHP.  The PHP proof of concept went fast - but noone in the group new either language.
We went with C#.

Other challenges - besides not knowing C# :)  
We had about 40,000 calls to make daily with data that was not available until just before working hours.
Initial runs were slow.  So I got the bug to add concurrency.  With my 2 weeks of c# experience this is what I came up with.

Even though the initial POC was written with an open/close to the database for each SQL I thought that seemed silly.
The database is integrated and managed on the power system.  
Turns out this based on some reading on stackoverflow that I'm not the first to have this idea.  
So next time and/or if any changes are need I know better.

Also missing is a real sense of OO. I've studied Java but never had a chance to use it in real time.  
