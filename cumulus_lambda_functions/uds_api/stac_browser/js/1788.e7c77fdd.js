"use strict";(self["webpackChunk_radiantearth_stac_browser"]=self["webpackChunk_radiantearth_stac_browser"]||[]).push([[1788],{81788:function(t,e,a){a.r(e),a.d(e,{default:function(){return p}});var l=function(){var t=this,e=t._self._c;return t.collection?e("section",{staticClass:"parent-collection card-list mb-4"},[e("h2",[t._v(t._s(t.$tc("stacCollection")))]),e("Catalog",{attrs:{catalog:t.collection,showThumbnail:t.showThumbnail}})],1):t._e()},c=[],n=a(54772),o=a(48416),i=a(40848),s={name:"CollectionLink",components:{Catalog:n.c},props:{link:{type:Object,required:!0},showThumbnail:{type:Boolean,default:!1}},computed:{...(0,o.gV)(["getStac"]),collection(){return this.getStac(this.link)}},watch:{link:{immediate:!0,handler(t){i.cp.isObject(t)&&this.$store.dispatch("load",{url:t.href})}}}},r=s,u=a(82528),h=(0,u.c)(r,l,c,!1,null,null,null),p=h.exports}}]);
//# sourceMappingURL=1788.e7c77fdd.js.map