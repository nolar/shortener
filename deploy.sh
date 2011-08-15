#!/bin/sh

sudo rm -Rf /var/www/shortener
sudo cp -R ~/Dropbox/Forge/shortener /var/www/
sudo chown -R root:root /var/www/shortener
sudo /etc/init.d/apache2 restart
