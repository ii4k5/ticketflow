# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Running the Project

```bash
# Start all services (preferred)
docker-compose up --build

# Start individual service locally (from its directory)
npm start          # production
npm run dev        # dev mode with nodemon (user-service only)
```

Exposed ports: Nginx → **80**, User Service → **3001**, Ticket Service → **3002**, Notification Service → **3003**, Kafka UI → **8080**, MySQL → **3306**

There is no test suite configured.

## Architecture Overview

TicketFlow is a microservices event-ticketing platform with four main services behind an Nginx reverse proxy.

### Services and Responsibilities

| Service | Port | Responsibility |
|---|---|---|
| `user-service` | 3001 | Auth (register/login/JWT), user management, RBAC permission checking |
| `ticket-service` | 3002 | Events CRUD, ticket booking/cancellation, seat availability |
| `notification-service` | 3003 | Kafka consumer that stores and serves notifications |
| `frontend` | — | Vanilla JS SPA served by Nginx |

### Request Flow

Browser → Nginx (:80) → routes by path prefix:
- `/api/users/*`, `/api/auth/*` → user-service
- `/api/events/*`, `/api/tickets/*` → ticket-service
- `/api/notifications/*` → notification-service
- `/` → static frontend

### Inter-Service Communication (Kafka)

Services communicate asynchronously via **Apache Kafka** (KafkaJS). Two topics:
- `user-events` — published by user-service on registration; consumed by ticket-service and notification-service
- `ticket-events` — published by ticket-service on booking/cancellation/low-availability; consumed by notification-service

Producers/consumers use retry logic (up to 10–15 retries, 3 s initial delay) to handle Kafka startup ordering.

### Authentication & Authorization

- JWT tokens signed with `JWT_SECRET` (default: `ticketflow-secret-2025`), stored client-side in `localStorage` as `tf_token`
- Three roles with hardcoded permission sets in `user-service/role.js`: **admin**, **manager**, **user**
- `ticket-service` also queries MySQL (`getEffectivePermissions`) to merge role + per-user permissions at request time

### Authentication Session Flow

- On login/register, `user-service` creates a row in the `sessions` table and sets an **HTTP-only cookie** (`tf_session`) — no token is stored in `localStorage`
- On page load, the frontend calls `GET /api/users/me` (cookie sent automatically by browser); the server validates the session from MySQL and returns a fresh JWT
- The JWT lives only in the JS variable `currentToken` (in-memory, cleared on tab close); it is sent as `Authorization: Bearer <token>` for requests to `ticket-service`
- `POST /api/users/logout` deletes the session row and clears the cookie

### Data Persistence

- **MySQL 8.0** — schema auto-created by `user-service` on startup via `initSchema()`; tables: `users`, `sessions`, `user_extra_permissions`
- **In-memory Maps** — `ticket-service` stores events and tickets in memory; data is lost on container restart
- `notification-service` is fully stateless (in-memory array)

### Admin Permission Grants

Admins can grant individual permissions to managers and users on top of their role's defaults, stored in `user_extra_permissions (user_id, permission)`. The ticket-service reads this table on every authenticated request to compute effective permissions.

## Known Issues

1. **In-memory state (ticket-service):** Events and tickets are seeded in-memory at startup; a container restart wipes all bookings.
