## ABOUT

Yet another web shortener, which is going to save internet traffic costs and world energy consumption.


## FEATURES

* Multi-domain hosted, with each domain isolated from the others, but sharing the infrastructure.
* HTML and JSON APIs (HTML is very useful for quick human-friendly experiments, see below).
* Supposed to handle really heavy traffic: with distributed ID generators (not implemented),
  de-centralized counters and storages, key-value storages, background analytics processing, etc.
* Stores its data in SimpleDB or MySQL (already works), or virtually any key-value capable storage.
* Core features are implemented as standalone Python 2.7 library (2.6 is okay too).
* Web entry points are made with Django 1.3.


## DEPLOYMENT

It is deployed at

  http://yaws.ws/

and all possible sub-domains:

  http://a.yaws.ws/
  http://9.yaws.ws/
  http://i.wanna.start.with.empty.yaws.ws/
  ...

Just go and try. And remember it is in very prototype stage, so error handling
may be weak (it just dies with unified error template), and data may be lost.


## HISTORY

This was one of the test tasks for one of the positions in one of the companies
in one of the countries on one of the globes I've applied to.
But I'm so easily carried away with interesting tasks (not the first time)...

Nevertheless, since it has grown to a working deployable project, here is the code
shared. You are free to join, to fork, to have fun.


## CREDITS

Author: Sergey Vasilyev <nolar@nolar.info>, http://nolar.info/
