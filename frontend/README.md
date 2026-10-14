# Relay dashboard

Next.js control room for the Python queue broker. Start with `npm ci` and `npm run dev`.

The default backend is http://127.0.0.1:5010. Copy `.env.example` to `.env.local` to change DJQ_API_URL. Route handlers forward allowlisted requests at runtime. Protected brokers require a token in Connection settings; credentials never enter the bundle.

Features: live counts, an eight-job demo, submission form, search/pagination, result inspection, dead-letter retry, worker capabilities/heartbeats, keyboard-accessible dialogs, mobile layouts, and loading/error/empty states.

Run `npm run lint`, `npm test`, and `npm run build` to verify. Docker and the root launcher use standalone production output. See the root README for the complete workflow.
