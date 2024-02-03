# chmod +x server.sh
clear && rm -f CLIENT_MANIFEST.MF && rm -f SERVER_MANIFEST.MF && ant && java -jar m1-server.jar 1057 1000 FIFO