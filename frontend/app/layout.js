import './globals.css';

export const metadata = {
  title: 'Relay — Queue control',
  description: 'A live control room for a durable distributed job queue. Submit, observe, retry, recover.',
};

export default function RootLayout({ children }) {
  return <html lang="en"><body>{children}</body></html>;
}
