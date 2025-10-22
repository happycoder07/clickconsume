#!/bin/bash
# Create an entrypoint script to run both services (libfsds and ipfixcol2)
echo "Starting ipfixcol2..."
/usr/local/bin/ipfixcol2 &

# Keep the container running
exec tail -f /dev/null
