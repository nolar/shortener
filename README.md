## ABOUT

Yet another URL shortener, which is going to safe internet traffic and energy consumption.


## FEATURES

* Multi-domain hosted, with each domain isolated from the other, but sharing the infrastructure.
* HTML and JSON APIs (HTML is for quick try, see below).
* Supposed to handle really heavy traffic: with distributed ID generators (not completely implemented),
  de-centralized counter and storages, background analytics processing, etc.
* Depends on Amazon Web Services (AWS), SimpleDB and SQS in particular.
* Coded with Python 2.7 (2.6 is okay too), Django 1.3.


## DEPLOYED

It is deployed at

  http://shortener.forge.nolar.info/

and all possible subdomains:

  http://abc.shortener.forge.nolar.info/
  http://def.shortener.forge.nolar.info/
  http://123.shortener.forge.nolar.info/
  ...

Just go and try. And remember it is in very prototype stage, so error handling
may be weak (it just dies with unified error template).


## HISTORY

This was one of the test tasks for one of the positions in one of the companies
in one of the countries on one of the globes I've applied to.
But I'm so easily carried away with interesting tasks (not the first time)...

Nevertheless, since it has grown to a working deployable project, here is the code
shared. You are free to join, to fork, to have fun.

PS: "Forge" is a folder where I work on proof-of-concept scripts and projects.
Now it is also a domain to host them for public. Full list is NOT available.
