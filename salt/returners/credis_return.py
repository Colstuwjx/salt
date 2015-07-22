# -*- coding: utf-8 -*-
'''
** credis version 0.1 **
** coder: jx.wu@ctrip.com **

Return data to a redis server.

To enable this returner the minion will need the python client for redis
installed and the following values configured in the minion or master
config, these are the defaults:

    redis.db: '0'
    redis.host: 'salt'
    redis.port: 6379

  To use the credis returner, append '--return credis' to the salt command. ex:

    salt '*' test.ping --return credis
'''

# Import python libs
import json

# Import Salt libs
import salt.utils

# Import third party libs
try:
    import redis
    HAS_REDIS = True
except ImportError:
    HAS_REDIS = False

# Define the module's virtual name
__virtualname__ = 'credis'


def __virtual__():
    if not HAS_REDIS:
        return False
    return __virtualname__


def _get_serv():
    '''
    Return a redis server object
    '''
    if 'config.option' in __salt__:
        return redis.Redis(
                host=__salt__['config.option']('redis.host'),
                port=__salt__['config.option']('redis.port'),
                db=__salt__['config.option']('redis.db'))
    else:
        cfg = __opts__
        return redis.Redis(
                host=cfg.get('redis.host', None),
                port=cfg.get('redis.port', None),
                db=cfg.get('redis.db', None))


def returner(ret):
    '''

    Return data to a redis data store

    this func is used for collect each minion job ret and return to job cache.

    '''
    serv = _get_serv()
    serv.set('{0}:{1}'.format(ret['id'], ret['jid']), json.dumps(ret))
    serv.lpush('{0}:{1}'.format(ret['id'], ret['fun']), ret['jid'])

    #TODO: there will be another minions which not-targeted by CkMinions, should check-in

    serv.sadd('minions', ret['id'])
    serv.sadd('jids', ret['jid'])


def save_load(jid, load):
    '''

    Save the load to the specified jid
    Add minions field to jid as the final targeted minions, if there is any syndic-master, just add to list.

    '''
    serv = _get_serv()

    ckminions = salt.utils.minions.CkMinions(__opts__)
    # Retrieve the minions list
    minions = ckminions.check_minions(
        load['tgt'],
        load.get('tgt_type', 'glob')
    )

    # Get the master id
    master_id = __opts__.get('master_id', None)

    # Try to get exist targeted minions
    # If it's syndic, just add to syndic list
    jinfo = get_load(jid)
    if jinfo:
        minions = list(set(minions) | set(jinfo.get("minions", [])))

    syndics = jinfo.get("syndics", [])
    if master_id is not None:
        syndics = list(set(syndics) | set([master_id]))

    load["syndics"] = syndics
    load["minions"] = minions
    serv.set(jid, json.dumps(load))
    serv.sadd('jids', jid)


def get_load(jid):
    '''
    Return the load data that marks a specified jid
    '''
    serv = _get_serv()
    data = serv.get(jid)
    if data:
        return json.loads(data)
    return {}


def get_jid(jid):
    '''

    Return the information returned when the specified job id was executed
    change to use spec jid target minions

    '''
    serv = _get_serv()
    ret = {}

    load = get_load(jid)
    if not load:
        return {}

    for minion in load['minions']:
        data = serv.get('{0}:{1}'.format(minion, jid))
        if data:
            ret[minion] = json.loads(data)

    return ret


def get_jid_minions(jid):
    '''

    Return all minions' keys as the spec jid

    '''
    serv = _get_serv()
    ret = []

    load = get_load(jid)
    return load.get('minions', [])


def get_fun(fun):
    '''
    Return a dict of the last function called for all minions
    '''
    serv = _get_serv()
    ret = {}
    for minion in serv.smembers('minions'):
        ind_str = '{0}:{1}'.format(minion, fun)
        try:
            jid = serv.lindex(ind_str, 0)
        except Exception:
            continue
        data = serv.get('{0}:{1}'.format(minion, jid))
        if data:
            ret[minion] = json.loads(data)
    return ret


def get_jids():
    '''
    Return a list of all job ids
    '''
    serv = _get_serv()
    return list(serv.smembers('jids'))


def get_minions():
    '''
    Return a list of minions
    '''
    serv = _get_serv()
    return list(serv.smembers('minions'))


def prep_jid(nocache, passed_jid=None):  # pylint: disable=unused-argument
    '''
    Do any work necessary to prepare a JID, including sending a custom id
    '''
    return passed_jid if passed_jid is not None else salt.utils.gen_jid()
