package samzaapps.ad;

import org.codehaus.jackson.annotate.JsonProperty;

/**
 * An ad click event.
 */
public class AdClick {

    private String pageId; // the unique id of the page that the ad was clicked on
    private String adId; // an unique id for the ad
    private String userId; // the user that clicked the ad

    public AdClick(
            @JsonProperty("pageId") String pageId,
            @JsonProperty("adId") String adId,
            @JsonProperty("userId") String userId) {
        this.pageId = pageId;
        this.adId = adId;
        this.userId = userId;
    }

    public String getPageId() {
        return pageId;
    }

    public void setPageId(String pageId) {
        this.pageId = pageId;
    }

    public String getAdId() {
        return adId;
    }

    public void setAdId(String adId) {
        this.adId = adId;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }
}