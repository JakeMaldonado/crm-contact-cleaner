from requests import get, delete
from time import sleep
from json import loads, dumps
from credentials import HUB_API_KEY, HUNT_API_KEY


def get_json(url, headers=None):
    '''
    makes a json get request and returns the response text
    :return res_text: the text responsne
    '''
    sleep(0.3)
    if headers:
        r = get(url, headers)
    else:
        r = get(url)
    if r.status_code != 200:
        print('Error: wrong status code ' + str(r.status_code))
        sleep(5)
        print('Sleeping... Trying again soon')
        r = get(url)
        if r.status_code != 200:
            print('Error: wrong status code ' + str(r.status_code))
            return None
        return loads(r.text)
    return loads(r.text)


def delete_json(url):
    '''
    send a delete request
    :param url: url to make request to
    :param payload: delete data
    :return status: 200 if success
    '''
    headers = {'content-type': 'application/json'}
    r = delete(url, headers=headers)
    if r.status_code != 200:
        print('Delete Error: Unexpected Status Code ' + str(r.status_code))
    return loads(r.text)


def offset_get_hubspot():
    '''
    gets all contacts by offsetting the requests
    :param url: url to pull from
    :return all_contacts: all hubspot contacts
    '''
    all_contacts = []
    url = 'https://api.hubapi.com/contacts/v1/lists/all/contacts/all?hapikey=' + HUB_API_KEY +'&count=100'
    r = get_json(url)
    new_contacts = r['contacts']

    while new_contacts:
        all_contacts.append(new_contacts.pop())

    print('Pulled ' + str(len(all_contacts)) + ' contacts')
    print('more? = ' + str(r['has-more']))

    has_more = r['has-more']
    offset = r['vid-offset']

    # pull contacts until we have all
    while has_more:
        print('another one')

        url = 'https://api.hubapi.com/contacts/v1/lists/all/contacts/all?hapikey=' + HUB_API_KEY + '&count=100' + '&vidOffset=' + str(offset)
        r = get_json(url)
        new_contacts = r['contacts']

        while new_contacts:
            all_contacts.append(new_contacts.pop())

        print('Pulled ' + str(len(all_contacts)) + ' contacts')

        has_more = r['has-more']
        offset = r['vid-offset']

    print(len(all_contacts))
    return all_contacts


def hunter_verify(email):
    '''
    validates an email using the hunter API
    :param email: email to verify
    :return is_valid: bool if email is valid
    '''
    url = 'https://api.hunter.io/v2/email-verifier?email=' + email + '&api_key=' + HUNT_API_KEY
    r = get_json(url)
    print(r)
    if r is not None:
        if 'data' in r:
            if 'result' in r['data']:
                if r['data']['result'] == 'deliverable' or r['data']['result'] == 'risky':
                    return True
    return False


def delete_from_hub(vid):
    '''
    Deletes contact from hubspot
    :param vid: id of contact to delete
    :return:
    '''
    url = 'https://api.hubapi.com/contacts/v1/contact/vid/' + str(vid) + '?hapikey=' + HUB_API_KEY
    r = delete_json(url)
    if r is None:
        print('Contact Error')
    else:
        print(r)
    return


def hub_contacted(vid):
    '''
    checks if contact has been contacted recently
    :param vid: ID of contact to check
    :return: bool if contact has been contacted
    '''
    url = 'https://api.hubapi.com/contacts/v1/contact/vid/' + str(vid) + '/profile?hapikey=' + HUB_API_KEY
    r = get_json(url)
    if 'num_unique_conversion_events' in r:
        if r['num_unique_conversion_events']['value'] != 0:
            return True
        else:
            return False
    return False


def get_contact_hub_email(vid):
    '''
    gets a contacts email form their vid
    :param vid: vid to search hubspot for
    :return: contacts email || None if not found/ Error
    '''
    url = 'https://api.hubapi.com/contacts/v1/contact/vid/' + str(vid) + '/profile?hapikey=' + HUB_API_KEY
    r = get_json(url)
    try:
        return r['properties']['email']['value']
    except Exception:
        print('Error: getting contact email -- VID: ' + str(vid))
    return None


def clean_db(to_clean):
    '''
    will clean the specified number of contacts in theDB
    :param to_clean: the amount of contacts to clean
    :return:
    '''
    all_contacts = offset_get_hubspot()
    clean_sum = to_clean - 1

    print(all_contacts)
    print('Cleaning ' + str(to_clean) + ' contacts in DB\n')

    # will only clean until to_clean --- to preserve requests
    while all_contacts and clean_sum > 0:
        contact = all_contacts.pop()
        vid = contact['vid']
        contacted = hub_contacted(vid)

        if not contacted:
            email = get_contact_hub_email(vid)
            if email is not None:
                keep = hunter_verify(email)
                if not keep:
                    delete_from_hub(vid)
                    print('Deleted ' + email)
                else:
                    print('Keeping contact ' + email)

        clean_sum -= 1

    print('Hub Clean Completed')
    return


clean_db(5480)
