import sys
import json
import os

basedir = os.path.realpath(os.path.dirname(__file__))
sys.path.append(os.path.join(basedir, "../../src/search"))

from design import Design

class SpencerDesign:
    def __init__(self):
        pass
    ## DEF
    
    def get_MMS_Design(self):
        d = Design()
        # collection mmsdbdays.data.rrdDays
        col_name = "mmsdbdays.data.rrdDays"
        d.addCollection(col_name)
        d.addIndex(col_name, ["_id"])
        d.addShardKey(col_name, ["_id"])
        
        # collection mmsdblogs-4f8dd12187d1d86fa8b99e50.acb986577e749f0d3c5b4e1de17e1e1a
        col_name = "mmsdblogs-4f8dd12187d1d86fa8b99e50.acb986577e749f0d3c5b4e1de17e1e1a"
        d.addCollection(col_name)
        d.addIndex(col_name, ["_id"])
        d.addShardKey(col_name, ["_id"])
        
        # collection mmsdblogs-4fa020bf87d1d86fa8b9eb6b.68df4b3652978efc8b58f6f6a5fd5fc5
        col_name = "mmsdblogs-4fa020bf87d1d86fa8b9eb6b.68df4b3652978efc8b58f6f6a5fd5fc5"
        d.addCollection(col_name)
        d.addIndex(col_name, ["_id"])
        d.addShardKey(col_name, ["_id"])
        
        # collection mmsdblogs-4fa020bf87d1d86fa8b9eb6b.397b8c68e7baa122099e9c0f420d0bf2
        col_name = "mmsdblogs-4fa020bf87d1d86fa8b9eb6b.397b8c68e7baa122099e9c0f420d0bf2"
        d.addCollection(col_name)
        d.addIndex(col_name, ["_id"])
        d.addShardKey(col_name, ["_id"])
        
        # collection mmsdb.data.agentAudits
        col_name = "mmsdb.data.agentAudits"
        d.addCollection(col_name)
        d.addIndex(col_name, ["_id"])
        d.addShardKey(col_name, ["_id"])
        
        # collection mmsdblogs-4fa020bf87d1d86fa8b9eb6b.754274d901dc95950d1f9fba2441be34
        col_name = "mmsdblogs-4fa020bf87d1d86fa8b9eb6b.754274d901dc95950d1f9fba2441be34:"
        d.addCollection(col_name)
        d.addIndex(col_name, ["_id"])
        d.addShardKey(col_name, ["_id"])
        
        # collection mmsdb.data.rrdMinutes
        col_name = "mmsdb.data.rrdMinutes"
        d.addCollection(col_name)
        d.addIndex(col_name, ["_id"])
        d.addIndex(col_name, ["d, cid, hid, g, i"])
        d.addShardKey(col_name, ["_id"])
        
        # collection mmsdblogs-4fa020bf87d1d86fa8b9eb6b.aa2d0cd7e5420c5c807ad6be8d22a7ba
        col_name = "mmsdblogs-4fa020bf87d1d86fa8b9eb6b.aa2d0cd7e5420c5c807ad6be8d22a7ba"
        d.addCollection(col_name)
        d.addIndex(col_name, ["_id"])
        d.addShardKey(col_name, ["_id"])
        
        # collection mmsdblogs-4fa020bf87d1d86fa8b9eb6b.d9d289019f38a50c8278a505584f6a82
        col_name = "mmsdblogs-4fa020bf87d1d86fa8b9eb6b.d9d289019f38a50c8278a505584f6a82"
        d.addCollection(col_name)
        d.addIndex(col_name, ["_id"])
        d.addShardKey(col_name, ["_id"])
        
        # collection mmsdbhours.data.rrdHours
        col_name = "mmsdbhours.data.rrdHours"
        d.addCollection(col_name)
        d.addIndex(col_name, ["_id"])
        d.addShardKey(col_name, ["_i"])
        
        print d.toJSON()
    ## DEF
    
    def get_TATP_Design(self):
        d = Design()
        # collection SUBSCRIBER
        col_name = "SUBSCRIBER"
        d.addCollection(col_name)
        d.addIndex(col_name, ["s_id"])
        d.addIndex(col_name, ["sub_nbr"])
        d.addShardKey(col_name, ["s_id"])
        
        # collection ACCESS_INFO
        col_name = "ACCESS_INFO"
        d.addCollection(col_name)
        d.addIndex(col_name, ["ai_type", "s_id"])
        d.addShardKey(col_name, ["ai_type," "s_id"])
        
        # collection SPECIAL_FACILITY
        col_name = "SPECIAL_FACILITY"
        d.addCollection(col_name)
        d.addIndex(col_name, ["s_id", "sf_type", "is_active"])
        d.addShardKey(col_name, ["s_id", "sf_type", "is_active"])
        
        # collection CALL_FORWARDING
        col_name = "CALL_FORWARDING"
        d.addCollection(col_name)
        d.addIndex(col_name, ["start_time", "sf_type", "s_id", "end_time"])
        d.addShardKey(col_name, ["start_time", "sf_type", "s_id", "end_time"])
        
        print d.toJSON()
    ## DEF
    
    def get_WORDPRESS_Design(self):
        d = Design()
        # collection wp_posts
        col_name = "wp_posts"
        d.addCollection(col_name)
        d.addIndex(col_name, ["ID", "post_type", "meta_key", "meta_value"])
        d.addIndex(col_name, ["post_type", "post_parent", "post_status"])
        d.addIndex(col_name, ["post_name", "post_status"])
        d.addIndex(col_name, ["post_status", "post_type", "post_date"])
        d.addIndex(col_name, ["ID", "post_name", "post_type", "post_parent"])
        d.addIndex(col_name, ["post_parent"])
        d.addShardKey(col_name, ["ID"])
        
        # collection wp_users
        col_name = "wp_users"
        d.addCollection(col_name)
        d.addIndex(col_name, ["user_login"])
        d.addIndex(col_name, ["ID"])
        d.addShardKey(col_name, ["user_login"])
        
        # collection wp_comments
        col_name = "wp_comments"
        d.addCollection(col_name)
        d.addIndex(col_name, ["comment_approved", "comment_post_ID"])
        d.addIndex(col_name, ["comment_post_ID"])
        d.addShardKey(col_name, ["comment_approved", "comment_post_ID"])
        
        # collection wp_options
        col_name = "wp_options"
        d.addCollection(col_name)
        d.addIndex(col_name, ["option_name"])
        d.addIndex(col_name, ["autoload"])
        d.addShardKey(col_name, ["option_name"])
        
        # collection wp_usermeta
        col_name = "wp_usermeta"
        d.addCollection(col_name)
        d.addIndex(col_name, ["user_id"])
        d.addIndex(col_name, ["meta_key"])
        d.addShardKey(col_name, ["user_id"])
        
        # collection wp_postmeta
        col_name = "wp_postmeta"
        d.addCollection(col_name)
        d.addIndex(col_name, ["post_id", "meta_key"])
        d.addShardKey(col_name, ["post_id", "meta_key"])
        
        print d.toJSON()
    ## DEF
    
    def get_WIKIPEDIA_Design(self):
        d = Design()
        # collection recentchanges
        col_name = "recentchanges"
        d.addCollection(col_name)
        d.addIndex(col_name, ["_id"])
        d.addShardKey(col_name, ["_id"])
        
        # collection logging
        col_name = "logging"
        d.addCollection(col_name)
        d.addIndex(col_name, ["_id"])
        d.addShardKey(col_name, ["_id"])
        
        # collection useracct
        col_name = "useracct"
        d.addCollection(col_name)
        d.addIndex(col_name, ["user_id"])
        d.addShardKey(col_name, ["user_id"])
        
        # collection text
        col_name = "text"
        d.addCollection(col_name)
        d.addIndex(col_name, ["_id"])
        d.addShardKey(col_name, ["_id"])
        
        # collection watchlist
        col_name = "watchlist"
        d.addCollection(col_name)
        d.addIndex(col_name, ["wl_namespace", "wl_notificationtimestamp", "wl_title", "wl_user", "j+WV+mk6"])
        d.addIndex(col_name, ["wl_namespace", "wl_notificationtimestamp", "wl_title", "wl_user", "A", "zLl6Bw"])
        d.addShardKey(col_name, ["wl_namespace", "wl_notificationtimestamp", "wl_title", "wl_user"])
        
        # collection page
        col_name = "page"
        d.addCollection(col_name)
        d.addIndex(col_name, ["page_namespace", "page_title"])
        d.addShardKey(col_name, ["page_namespace", "page_title"])
        
        # collection revision
        col_name = "revision"
        d.addCollection(col_name)
        d.addIndex(col_name, ["rev_page", "rev_id", "page_id"])
        d.addShardKey(col_name, ["rev_page", "rev_id", "page_id"])
        
        print d.toJSON()
    ## DEF
    
    def get_EXFM_Design(self):
        d = Design()
        # collection exfm.sotd
        col_name = "exfm.sotd"
        d.addCollection(col_name)
        d.addIndex(col_name, ["_id"])
        d.addShardKey(col_name, ["_id"])
        
        # collection exfm.site.followers
        col_name = "exfm.site.followers"
        d.addCollection(col_name)
        d.addIndex(col_name, ["_id"])
        d.addShardKey(col_name, ["_id"])
        
        # collection exfm.user.site
        col_name = "exfm.user.site"
        d.addCollection(col_name)
        d.addIndex(col_name, ["_id"])
        d.addShardKey(col_name, ["_id"])
        
        # collection exfm.user.meta
        col_name = "exfm.user.meta"
        d.addCollection(col_name)
        d.addIndex(col_name, ["_id"])
        d.addIndex(col_name, ["email"])
        d.addShardKey(col_name, ["_id"])
        
        # collection exfm.user.activity
        col_name = "exfm.user.activity"
        d.addCollection(col_name)
        d.addIndex(col_name, ["_id"])
        d.addIndex(col_name, ["verb", "_id"])
        d.addIndex(col_name, ["verb", "username", "obj_id"])
        d.addIndex(col_name, ["verb", "username", "created"])
        d.addShardKey(col_name, ["verb", "_id"])
        
        # collection exfm.user.loved
        col_name = "exfm.user.loved"
        d.addCollection(col_name)
        d.addIndex(col_name, ["_id"])
        d.addShardKey(col_name, ["_id"])
        
        # collection exfm.user.following
        col_name = "exfm.user.following"
        d.addCollection(col_name)
        d.addIndex(col_name, ["_id"])
        d.addShardKey(col_name, ["_id"])
        
        # collection exfm.user.service
        col_name = "exfm.user.service"
        d.addCollection(col_name)
        d.addIndex(col_name, ["_id"])
        d.addShardKey(col_name, ["_id"])
        
        # collection exfm.song.site
        col_name = "exfm.song.site"
        d.addCollection(col_name)
        d.addIndex(col_name, ["_id"])
        d.addShardKey(col_name, ["_id"])
        
        # collection exfm.site.songs
        col_name = "exfm.site.songs"
        d.addCollection(col_name)
        d.addIndex(col_name, ["_id"])
        d.addShardKey(col_name, ["_id"])
        
        # collection exfm.user.recently_viewed_site
        col_name = "exfm.user.recently_viewed_site"
        d.addCollection(col_name)
        d.addIndex(col_name, ["_id"])
        d.addShardKey(col_name, ["_id"])
        
        # collection exfm.site.song_publish_dates
        col_name = "exfm.site.song_publish_dates"
        d.addCollection(col_name)
        d.addIndex(col_name, ["_id", "p"])
        d.addShardKey(col_name, ["p"]) # If insert volume low, shard on p, if high shard on _id
        
        # collecion exfm.user.followers
        col_name = "exfm.user.followers"
        d.addCollection(col_name)
        d.addIndex(col_name, ["_id"])
        d.addShardKey(col_name, ["_id"])
        
        # collection exfm.artist.meta
        col_name = "exfm.artist.meta"
        d.addCollection(col_name)
        d.addIndex(col_name, ["_id"])
        d.addShardKey(col_name, ["_id"])
        
        # collection exfm.user.feed
        col_name = "exfm.user.feed"
        d.addCollection(col_name)
        d.addIndex(col_name, ["username", "verb"])
        d.addIndex(col_name, ["verb", "actor", "obj_id"])
        d.addShardKey(col_name, ["username", "verb"])
        
        # collection exfm.site.meta
        col_name = "exfm.site.meta"
        d.addCollection(col_name)
        d.addIndex(col_name, ["_id"])
        d.addShardKey(col_name, ["_id"])
        
        # collection exfm.song.meta
        col_name = "exfm.song.meta"
        d.addCollection(col_name)
        d.addIndex(col_name, ["_id"])
        d.addIndex(col_name, ["md5"])
        d.addShardKey(col_name, ["_id"])
        
        print d.toJSON()
    ## DEF
## CLASS    
if __name__ == '__main__':
    s = SpencerDesign()
    #s.get_MMS_Design()
    #s.get_TATP_Design()
    #s.get_EXFM_Design()
    #s.get_WIKIPEDIA_Design()
    s.get_WORDPRESS_Design()