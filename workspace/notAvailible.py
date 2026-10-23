servers = [A, B, C]
majority = len(servers)

def write(key, value(:
    if not is-leader():
        return REDIRECT_TO_LEADER

    entry = (current_term, key, value)
    append_to_locallog(entry)

    acknowledgements = 1

    for follower in followers:
        if replicate_log_to(follower, entry):
            acknowledgments += 1
    if acknowledgments >= 
