import argparse
import json
import random
import threading
import time
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

PORTS = {'A': 8101, 'B': 8102, 'C': 8103}
MAJORITY = len(PORTS) // 2 + 1

class Node:
    def __init__(self, name):
        self.name = name
        self.peers = [p for p in PORTS if p != name]
        self.lock = threading.RLock()
        self.term = 0
        self.voted_for = None
        self.role = 'follower'
        self.leader = None
        self.log = []  # entries: {'term', 'key', 'value'}
        self.commit_index = -1
        self.applied_index = -1
        self.db = {}
        self.deadline = 0
        self.reset_deadline()
        self.next_index = {p: 0 for p in self.peers}
        self.match_index = {p: -1 for p in self.peers}
        self.last_heartbeat = 0
        self.write_lock = threading.Lock()

    def reset_deadline(self):
        self.deadline = time.monotonic() + random.uniform(1.2, 2.4)

    def rpc(self, peer, path, payload, timeout=0.4):
        url = f'http://127.0.0.1:{PORTS[peer]}{path}'
        req = urllib.request.Request(url, data=json.dumps(payload).encode(), headers={'Content-Type':'application/json'})
        try:
            with urllib.request.urlopen(req, timeout=timeout) as res:
                return json.load(res)
        except (urllib.error.URLError, TimeoutError, OSError, ValueError):
            return None

    def last_info(self):
        return (len(self.log)-1, self.log[-1]['term'] if self.log else 0)

    def step_down(self, term):
        if term > self.term:
            self.term = term
            self.voted_for = None
        self.role = 'follower'
        self.leader = None
        self.reset_deadline()

    def vote(self, msg):
        with self.lock:
            term = msg['term']
            if term > self.term:
                self.step_down(term)
            if term < self.term:
                return {'term': self.term, 'granted': False}
            my_index, my_term = self.last_info()
            up_to_date = (msg['last_term'], msg['last_index']) >= (my_term, my_index)
            granted = (self.voted_for in (None, msg['candidate'])) and up_to_date
            if granted:
                self.voted_for = msg['candidate']
                self.reset_deadline()
            return {'term': self.term, 'granted': granted}

    def apply_committed(self):
        while self.applied_index < self.commit_index:
            self.applied_index += 1
            entry = self.log[self.applied_index]
            self.db[entry['key']] = entry['value']

    def append_entries(self, msg):
        with self.lock:
            term = msg['term']
            if term > self.term:
                self.step_down(term)
            if term < self.term:
                return {'term': self.term, 'success': False, 'match': -1}
            self.role = 'follower'
            self.leader = msg['leader']
            self.reset_deadline()
            prev = msg['prev_index']
            if prev >= len(self.log) or (prev >= 0 and self.log[prev]['term'] != msg['prev_term']):
                return {'term': self.term, 'success': False, 'match': -1}
            i = prev + 1
            for entry in msg['entries']:
                if i < len(self.log) and self.log[i]['term'] != entry['term']:
                    self.log = self.log[:i]
                if i == len(self.log):
                    self.log.append(entry)
                i += 1
            self.commit_index = min(msg['commit'], len(self.log)-1) if msg['commit'] > self.commit_index else self.commit_index
            self.apply_committed()
            return {'term': self.term, 'success': True, 'match': i-1}

    def election(self):
        with self.lock:
            if self.role == 'leader':
                return
            self.role = 'candidate'
            self.term += 1
            term = self.term
            self.voted_for = self.name
            self.leader = None
            self.reset_deadline()
            index, last_term = self.last_info()
        votes = 1
        for peer in self.peers:
            res = self.rpc(peer, '/vote', {'term': term, 'candidate': self.name, 'last_index': index, 'last_term': last_term})
            with self.lock:
                if res and res['term'] > self.term:
                    self.step_down(res['term'])
                    return
                if self.term != term or self.role != 'candidate':
                    return
                if res and res['granted']:
                    votes += 1
        with self.lock:
            if self.term == term and self.role == 'candidate' and votes >= MAJORITY:
                self.role = 'leader'
                self.leader = self.name
                self.next_index = {p: len(self.log) for p in self.peers}
                self.match_index = {p: -1 for p in self.peers}
                self.last_heartbeat = 0
                print(f'{self.name}: elected leader for term {term}', flush=True)

    def replicate(self, peer):
        with self.lock:
            if self.role != 'leader':
                return False
            term = self.term
            nxt = self.next_index[peer]
            prev = nxt-1
            msg = {'term': term, 'leader': self.name, 'prev_index': prev,
                   'prev_term': self.log[prev]['term'] if prev >= 0 else 0,
                   'entries': self.log[nxt:], 'commit': self.commit_index}
        res = self.rpc(peer, '/append', msg)
        with self.lock:
            if res and res['term'] > self.term:
                self.step_down(res['term'])
                return False
            if self.role != 'leader' or self.term != term:
                return False
            if res and res['success']:
                self.match_index[peer] = max(self.match_index[peer], res['match'])
                self.next_index[peer] = self.match_index[peer] + 1
                return True
            if res:
                self.next_index[peer] = max(0, nxt-1)
            return False

    def advance_commit(self):
        with self.lock:
            if self.role != 'leader':
                return
            for i in range(len(self.log)-1, self.commit_index, -1):
                if self.log[i]['term'] != self.term:
                    continue
                count = 1 + sum(self.match_index[p] >= i for p in self.peers)
                if count >= MAJORITY:
                    self.commit_index = i
                    self.apply_committed()
                    break

    def heartbeat_round(self):
        for peer in self.peers:
            self.replicate(peer)
        self.advance_commit()

    def confirm_read(self):
        # Heartbeat majority in current term, then read only applied committed state.
        with self.lock:
            if self.role != 'leader':
                return False
            term = self.term
        count = 1
        for peer in self.peers:
            if self.replicate(peer):
                count += 1
        with self.lock:
            return self.role == 'leader' and self.term == term and count >= MAJORITY and self.applied_index >= self.commit_index

    def write(self, key, value):
        with self.write_lock:
            with self.lock:
                if self.role != 'leader':
                    return {'error': 'not leader', 'leader': self.leader}, 409
                term = self.term
                self.log.append({'term': term, 'key': key, 'value': value})
                target = len(self.log)-1
            end = time.monotonic() + 2.0
            while time.monotonic() < end:
                self.heartbeat_round()
                with self.lock:
                    if self.role != 'leader' or self.term != term:
                        return {'error': 'leadership changed; outcome unknown'}, 503
                    if self.commit_index >= target:
                        return {'ok': True, 'index': target, 'leader': self.name}, 200
                time.sleep(0.08)
            return {'error': 'not committed yet; outcome unknown'}, 503

    def run_background(self):
        while True:
            with self.lock:
                leader = self.role == 'leader'
                election_due = not leader and time.monotonic() >= self.deadline
                beat_due = leader and time.monotonic() - self.last_heartbeat >= 0.15
                if beat_due:
                    self.last_heartbeat = time.monotonic()
            if election_due:
                self.election()
            if beat_due:
                self.heartbeat_round()
            time.sleep(0.05)


def handler_factory(node):
    class Handler(BaseHTTPRequestHandler):
        def send(self, data, status=200):
            body = json.dumps(data).encode()
            self.send_response(status)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_POST(self):
            try:
                msg = json.loads(self.rfile.read(int(self.headers.get('Content-Length', '0'))))
                if self.path == '/vote':
                    self.send(node.vote(msg))
                elif self.path == '/append':
                    self.send(node.append_entries(msg))
                elif self.path == '/write':
                    if 'key' not in msg or 'value' not in msg:
                        self.send({'error': 'key and value required'}, 400)
                    else:
                        result, status = node.write(str(msg['key']), msg['value'])
                        self.send(result, status)
                else:
                    self.send({'error': 'not found'}, 404)
            except (ValueError, KeyError, TypeError) as exc:
                self.send({'error': str(exc)}, 400)

        def do_GET(self):
            if self.path == '/status':
                with node.lock:
                    self.send({'node': node.name, 'role': node.role, 'term': node.term,
                               'leader': node.leader, 'commit_index': node.commit_index,
                               'log': list(node.log), 'database': dict(node.db)})
            elif self.path.startswith('/read/'):
                key = self.path[len('/read/'):]
                with node.write_lock:
                    if not node.confirm_read():
                        self.send({'error': 'no verified leader majority', 'leader': node.leader}, 503)
                        return
                    with node.lock:
                        if key not in node.db:
                            self.send({'error': 'key not found'}, 404)
                        else:
                            self.send({'key': key, 'value': node.db[key]})
            else:
                self.send({'error': 'not found'}, 404)

        def log_message(self, fmt, *args):
            pass
    return Handler

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('node', choices=PORTS)
    args = parser.parse_args()
    node = Node(args.node)
    threading.Thread(target=node.run_background, daemon=True).start()
    print(f'{node.name} listening on 127.0.0.1:{PORTS[node.name]}', flush=True)
    ThreadingHTTPServer(('127.0.0.1', PORTS[node.name]), handler_factory(node)).serve_forever()
