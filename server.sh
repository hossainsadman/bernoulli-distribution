# chmod +x server.sh && chmod +x test.sh
clear && rm -f CLIENT_MANIFEST.MF && rm -f SERVER_MANIFEST.MF && rm -f ECS_MANIFEST.MF && ant && java -jar m3-server.jar 1057 5 FIFO "$@" 