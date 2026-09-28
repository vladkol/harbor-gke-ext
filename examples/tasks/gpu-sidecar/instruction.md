# GPU sidecar inside the DinD plane

Two sidecars are running alongside this container:

- `model` on port **8080**, which holds a GPU reservation
- `mini-service` on port **8080**

Both expose the same port, so Harbor routes them into the nested Docker plane.

Collect their status and write it to disk:

1. `GET http://model:8080/status` → write the JSON body to `/app/model_status.json`
2. `GET http://mini-service:8080/health` → write the JSON body to `/app/mini_service_status.json`
