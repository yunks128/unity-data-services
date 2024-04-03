"use strict";(self["webpackChunk_radiantearth_stac_browser"]=self["webpackChunk_radiantearth_stac_browser"]||[]).push([[7944],{97944:function(t,e,r){r.r(e),r.d(e,{default:function(){return g}});var n=function(){var t=this,e=t._self._c;return e("main",{staticClass:"select-data-source"},[e("b-form",{on:{submit:t.go}},[e("b-form-group",{attrs:{id:"select",label:t.$t("index.specifyCatalog"),"label-for":"url","invalid-feedback":t.error,state:t.valid}},[e("b-form-input",{attrs:{id:"url",type:"url",value:t.url,placeholder:"https://..."},on:{input:t.setUrl}})],1),e("b-button",{attrs:{type:"submit",variant:"primary"}},[t._v(t._s(t.$t("index.load")))])],1),t.stacIndex.length>0?e("hr"):t._e(),t.stacIndex.length>0?e("b-form-group",{staticClass:"stac-index",scopedSlots:t._u([{key:"label",fn:function(){return[e("i18n",{attrs:{path:"index.selectStacIndex"},scopedSlots:t._u([{key:"stacIndex",fn:function(){return[e("a",{attrs:{href:"https://stacindex.org",target:"_blank"}},[t._v("STAC Index")])]},proxy:!0}],null,!1,4016002706)})]},proxy:!0}],null,!1,2418002653)},[e("b-list-group",[t._l(t.stacIndex,(function(r){return[t.show(r)?e("b-list-group-item",{key:r.id,attrs:{button:"",active:t.url===r.url},on:{click:function(e){return t.open(r.url)}}},[e("div",{staticClass:"d-flex justify-content-between align-items-baseline mb-1"},[e("strong",[t._v(t._s(r.title))]),r.isApi?e("b-badge",{attrs:{variant:"danger"}},[t._v(t._s(t.$t("index.api")))]):e("b-badge",{attrs:{variant:"success"}},[t._v(t._s(t.$t("index.catalog")))])],1),e("Description",{attrs:{description:r.summary,compact:""}})],1):t._e()]}))],2)],1):t._e()],1)},a=[],i=(r(83248),r(12168),r(35104),r(88312),r(96296)),o=r(60592),s=r(46584),u=r(50144),l=r(32872),c=r(48416),d=r(44093),p=r(40848),f={name:"SelectDataSource",components:{BForm:i.E,BFormGroup:o.K,BFormInput:s.U,BListGroup:u.u,BListGroupItem:l.k,Description:d["default"]},data(){return{url:"",stacIndex:[]}},computed:{...(0,c.gV)(["toBrowserPath"]),valid(){return!this.error},error(){if(!this.url)return null;try{let t=new URL(this.url);return t.protocol?t.host?null:this.$t("index.urlMissingHost"):this.$t("index.urlMissingProtocol")}catch(t){return this.$t("index.urlInvalid")}}},async created(){this.$store.commit("resetCatalog",!0);try{this.stacIndex=[{id:1,url:"https://d3vc8w9zcq658.cloudfront.net/sbx-uds-dapa/catalog",slug:"unity-ds",title:"Unity DS (DEV SANDBOX)",summary:"Unity DS in Sandbox Environment where things will break down most of the time",access:"public",created:"2023-03-16T09:15:31.242Z",updated:"2023-03-16T09:15:31.242Z",isPrivate:!1,isApi:!1,accessInfo:null},{id:2,url:"https://d3vc8w9zcq658.cloudfront.net/am-uds-dapa/catalog",slug:"unity-ds",title:"Unity DS (DEV STABLE)",summary:"Unity DS in Dev Environment.",access:"public",created:"2023-03-16T09:15:31.242Z",updated:"2023-03-16T09:15:31.242Z",isPrivate:!1,isApi:!1,accessInfo:null},{id:3,url:"https://dxebrgu0bc9w7.cloudfront.net/am-uds-dapa/catalog",slug:"unity-ds",title:"Unity DS (TEST)",summary:"Unity DS in Test Environment.",access:"public",created:"2023-03-16T09:15:31.242Z",updated:"2023-03-16T09:15:31.242Z",isPrivate:!1,isApi:!1,accessInfo:null},{id:4,url:"https://d2zjsabg0fonik.cloudfront.net/am-uds-dapa/catalog",slug:"unity-ds",title:"Unity DS (Production)",summary:"Unity DS in Production Environment.",access:"public",created:"2023-03-16T09:15:31.242Z",updated:"2023-03-16T09:15:31.242Z",isPrivate:!1,isApi:!1,accessInfo:null}]}catch(t){console.error(t)}},methods:{show(t){return"private"!==t.access&&(!this.url||p.cp.search(this.url,[t.title,t.url]))},setUrl(t){this.url=t},open(t){this.url=t,this.go()},go(){this.$router.push(this.toBrowserPath(this.url))}}},h=f,v=r(82528),b=(0,v.c)(h,n,a,!1,null,null,null),g=b.exports},96296:function(t,e,r){r.d(e,{E:function(){return l}});var n=r(77548),a=r(76516),i=r(99628),o=r(95756),s=r(97637),u=(0,s.a8)({id:(0,s.K2)(o.nV),inline:(0,s.K2)(o.aM,!1),novalidate:(0,s.K2)(o.aM,!1),validated:(0,s.K2)(o.aM,!1)},i.U$),l=(0,n.SU)({name:i.U$,functional:!0,props:u,render:function(t,e){var r=e.props,n=e.data,i=e.children;return t("form",(0,a.k)(n,{class:{"form-inline":r.inline,"was-validated":r.validated},attrs:{id:r.id,novalidate:r.novalidate}}),i)}})},32872:function(t,e,r){r.d(e,{k:function(){return y}});var n=r(77548),a=r(76516),i=r(99628),o=r(95756),s=r(33272),u=r(53377),l=r(25800),c=r(97637),d=r(71680),p=r(10944);function f(t,e){var r=Object.keys(t);if(Object.getOwnPropertySymbols){var n=Object.getOwnPropertySymbols(t);e&&(n=n.filter((function(e){return Object.getOwnPropertyDescriptor(t,e).enumerable}))),r.push.apply(r,n)}return r}function h(t){for(var e=1;e<arguments.length;e++){var r=null!=arguments[e]?arguments[e]:{};e%2?f(Object(r),!0).forEach((function(e){v(t,e,r[e])})):Object.getOwnPropertyDescriptors?Object.defineProperties(t,Object.getOwnPropertyDescriptors(r)):f(Object(r)).forEach((function(e){Object.defineProperty(t,e,Object.getOwnPropertyDescriptor(r,e))}))}return t}function v(t,e,r){return e in t?Object.defineProperty(t,e,{value:r,enumerable:!0,configurable:!0,writable:!0}):t[e]=r,t}var b=["a","router-link","button","b-link"],g=(0,l.qn)(p.ov,["event","routerTag"]);delete g.href.default,delete g.to.default;var m=(0,c.a8)((0,l.UJ)(h(h({},g),{},{action:(0,c.K2)(o.aM,!1),button:(0,c.K2)(o.aM,!1),tag:(0,c.K2)(o.nV,"div"),variant:(0,c.K2)(o.nV)})),i.EB),y=(0,n.SU)({name:i.EB,functional:!0,props:m,render:function(t,e){var r,n=e.props,i=e.data,o=e.children,l=n.button,f=n.variant,h=n.active,m=n.disabled,y=(0,d.SQ)(n),w=l?"button":y?p.Yr:n.tag,S=!!(n.action||y||l||(0,s.ot)(b,n.tag)),x={},k={};return(0,u.K8)(w,"button")?(i.attrs&&i.attrs.type||(x.type="button"),n.disabled&&(x.disabled=!0)):k=(0,c.sn)(g,n),t(w,(0,a.k)(i,{attrs:x,props:k,staticClass:"list-group-item",class:(r={},v(r,"list-group-item-".concat(f),f),v(r,"list-group-item-action",S),v(r,"active",h),v(r,"disabled",m),r)}),o)}})},50144:function(t,e,r){r.d(e,{u:function(){return d}});var n=r(77548),a=r(76516),i=r(99628),o=r(95756),s=r(77928),u=r(97637);function l(t,e,r){return e in t?Object.defineProperty(t,e,{value:r,enumerable:!0,configurable:!0,writable:!0}):t[e]=r,t}var c=(0,u.a8)({flush:(0,u.K2)(o.aM,!1),horizontal:(0,u.K2)(o.wz,!1),tag:(0,u.K2)(o.nV,"div")},i.So),d=(0,n.SU)({name:i.So,functional:!0,props:c,render:function(t,e){var r=e.props,n=e.data,i=e.children,o=""===r.horizontal||r.horizontal;o=!r.flush&&o;var u={staticClass:"list-group",class:l({"list-group-flush":r.flush,"list-group-horizontal":!0===o},"list-group-horizontal-".concat(o),(0,s.ct)(o))};return t(r.tag,(0,a.k)(n,u),i)}})},38040:function(t,e,r){var n=r(41712),a=r(69063),i=r(75983),o=r(81840),s=o("toStringTag"),u=Object,l="Arguments"===i(function(){return arguments}()),c=function(t,e){try{return t[e]}catch(r){}};t.exports=n?i:function(t){var e,r,n;return void 0===t?"Undefined":null===t?"Null":"string"==typeof(r=c(e=u(t),s))?r:l?i(e):"Object"===(n=i(e))&&a(e.callee)?"Arguments":n}},41720:function(t,e,r){var n=r(50316),a=r(50368);t.exports=function(t,e,r){return r.get&&n(r.get,e,{getter:!0}),r.set&&n(r.set,e,{setter:!0}),a.f(t,e,r)}},41712:function(t,e,r){var n=r(81840),a=n("toStringTag"),i={};i[a]="z",t.exports="[object z]"===String(i)},91992:function(t,e,r){var n=r(38040),a=String;t.exports=function(t){if("Symbol"===n(t))throw new TypeError("Cannot convert a Symbol value to a string");return a(t)}},3416:function(t){var e=TypeError;t.exports=function(t,r){if(t<r)throw new e("Not enough arguments");return t}},12168:function(t,e,r){var n=r(63244),a=r(11447),i=r(91992),o=r(3416),s=URLSearchParams,u=s.prototype,l=a(u.append),c=a(u["delete"]),d=a(u.forEach),p=a([].push),f=new s("a=1&a=2&b=3");f["delete"]("a",1),f["delete"]("b",void 0),f+""!=="a=2"&&n(u,"delete",(function(t){var e=arguments.length,r=e<2?void 0:arguments[1];if(e&&void 0===r)return c(this,t);var n=[];d(this,(function(t,e){p(n,{key:e,value:t})})),o(e,1);var a,s=i(t),u=i(r),f=0,h=0,v=!1,b=n.length;while(f<b)a=n[f++],v||a.key===s?(v=!0,c(this,a.key)):h++;while(h<b)a=n[h++],a.key===s&&a.value===u||l(this,a.key,a.value)}),{enumerable:!0,unsafe:!0})},35104:function(t,e,r){var n=r(63244),a=r(11447),i=r(91992),o=r(3416),s=URLSearchParams,u=s.prototype,l=a(u.getAll),c=a(u.has),d=new s("a=1");!d.has("a",2)&&d.has("a",void 0)||n(u,"has",(function(t){var e=arguments.length,r=e<2?void 0:arguments[1];if(e&&void 0===r)return c(this,t);var n=l(this,t);o(e,1);var a=i(r),s=0;while(s<n.length)if(n[s++]===a)return!0;return!1}),{enumerable:!0,unsafe:!0})},88312:function(t,e,r){var n=r(83528),a=r(11447),i=r(41720),o=URLSearchParams.prototype,s=a(o.forEach);n&&!("size"in o)&&i(o,"size",{get:function(){var t=0;return s(this,(function(){t++})),t},configurable:!0,enumerable:!0})}}]);
//# sourceMappingURL=7944.a7540bf6.js.map